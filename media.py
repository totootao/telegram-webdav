"""媒体时长解析（纯标准库，零第三方依赖）。

为什么是本地解析，而不是让 Telegram 帮我们算：
    Telegram 的 ``sendAudio`` / ``sendVideo`` 会解析媒体并在响应里回带 ``duration``，
    但这条路走不通——
    1. ``sendVideo`` 会**转码**视频，下载到的字节 != 上传的字节，直接破坏
       SHA-256 完整性校验（见 webdav.py 的逐片校验）；
    2. 文件被切成 ``CHUNK_SIZE_MB`` 大小的分片后，每片都是"任意字节片段"，
       根本不是合法媒体文件，``sendAudio``/``sendVideo`` 无从解析。
    所以时长在本地算，Telegram 侧仍按 ``application/octet-stream`` 原字节存储，
    存储与传输链路一行不改。

覆盖格式：
    - MP4 / M4A / M4B / M4V / MOV / 3GP：ISO BMFF，取 ``moov``→``mvhd`` 的
      timescale / duration
    - MP3：优先读 Xing / Info 头的帧数（精确）；无 Xing 时按 CBR 比特率估算
    - WAV：RIFF ``fmt `` 的字节率 + ``data`` 块大小
    - FLAC：``fLaC`` 后第一个 STREAMINFO 块的 sample rate + total samples

设计要点：
    - 只读文件**头部/尾部的采样区**（各几百 KB ~ 几 MB），O(采样) 解析，绝不全文扫描；
    - 任何异常都吞掉返回 ``None``——解析失败绝不能影响上传主流程；
    - 返回秒（float），无法判断时返回 ``None``。
"""

import struct

# MP3 比特率表（kbps），按 (是否 MPEG1, layer) 索引，数组下标即帧头里的 bitrate index
_MP3_BITRATES = {
    (True, 1): [0, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448],
    (True, 2): [0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384],
    (True, 3): [0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320],
    (False, 1): [0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256],
    (False, 2): [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160],
    (False, 3): [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160],
}
# MP3 采样率表，按版本索引（下标即帧头里的 sampling rate index）
_MP3_SR = {
    3: [44100, 48000, 32000],   # MPEG1
    2: [22050, 24000, 16000],   # MPEG2
    0: [11025, 12000, 8000],    # MPEG2.5
}
# 常见容器魔数（ftyp 家族）
_FTYP_LIKE = (b"ftyp", b"moov", b"mdat", b"free", b"skip", b"wide", b"pnot")


# ---------------------------------------------------------------- MP4 家族
def _mp4(head, tail):
    """ISO BMFF：在 head/tail 采样里找 ``mvhd``，取 timescale 与 duration。

    ``moov`` 可能在文件头（faststart）也可能在文件尾，而 ``mvhd`` 位于 ``moov`` 的
    开头，所以头尾采样各搜一遍即可覆盖两种情况。
    """
    for data in (head, tail):
        if not data:
            continue
        pos = 0
        while True:
            i = data.find(b"mvhd", pos)
            if i < 0:
                break
            p = i + 4  # box payload 起点（前面 4 字节是 version+flags 之前的 box header）
            if p + 20 > len(data):
                break
            ver = data[p]
            if ver == 0:
                # version 0：creation(4) modification(4) timescale(4) duration(4)
                ts = int.from_bytes(data[p + 12:p + 16], "big")
                du = int.from_bytes(data[p + 16:p + 20], "big")
            elif ver == 1:
                # version 1：creation(8) modification(8) timescale(4) duration(8)
                if p + 32 > len(data):
                    break
                ts = int.from_bytes(data[p + 20:p + 24], "big")
                du = int.from_bytes(data[p + 24:p + 32], "big")
            else:
                pos = i + 4
                continue
            # 合理性校验，避免把 mdat 里的巧合字节当成 mvhd
            if 100 <= ts <= 10_000_000 and du > 0 and du / ts < 172800:
                return du / ts
            pos = i + 4
    return None


# ---------------------------------------------------------------- MP3
def _mp3_frame(b, off):
    """解析 off 处的 MP3 帧头，返回 (bitrate_bps, sample_rate, samples_per_frame, side_info_size)。"""
    if off + 4 > len(b):
        return None
    if b[off] != 0xFF or (b[off + 1] & 0xE0) != 0xE0:
        return None
    ver = (b[off + 1] >> 3) & 0x03
    layer_bits = (b[off + 1] >> 1) & 0x03
    br_idx = (b[off + 2] >> 4) & 0x0F
    sr_idx = (b[off + 2] >> 2) & 0x03
    # ver=1 保留值、layer=0 保留值、br 0/15 非法、sr 3 保留值
    if ver == 1 or layer_bits == 0 or br_idx in (0, 15) or sr_idx == 3:
        return None
    is_mpeg1 = ver == 3
    layer = 4 - layer_bits  # 0b01->Layer3, 0b10->Layer2, 0b11->Layer1
    br = _MP3_BITRATES.get((is_mpeg1, layer), [0] * 15)[br_idx] * 1000
    sr = _MP3_SR.get(ver, [0, 0, 0])[sr_idx]
    if layer == 1:
        spf = 384
    elif layer == 2:
        spf = 1152
    else:
        spf = 1152 if is_mpeg1 else 576
    mono = ((b[off + 3] >> 6) & 0x03) == 3
    if is_mpeg1:
        side = 17 if mono else 32
    else:
        side = 9 if mono else 17
    return br, sr, spf, side


def _mp3(head, size):
    n = len(head)
    off = 0
    # 跳过 ID3v2 标签（size 是 synchsafe 的 28 位整数）
    if head[:3] == b"ID3" and n >= 10:
        t = head[6:10]
        off = 10 + (((t[0] & 0x7F) << 21) | ((t[1] & 0x7F) << 14)
                    | ((t[2] & 0x7F) << 7) | (t[3] & 0x7F))
    # 找第一个合法帧头
    info = start = None
    limit = min(n - 4, off + 200000)
    i = max(0, off)
    while i <= limit:
        f = _mp3_frame(head, i)
        if f:
            info, start = f, i
            break
        i += 1
    if info is None:
        return None
    br, sr, spf, side = info
    if not br or not sr:
        return None
    # Xing / Info 头：带总帧数，可精确计算（VBR 也准）
    x = start + 4 + side
    if x + 12 <= n and head[x:x + 4] in (b"Xing", b"Info"):
        flags = int.from_bytes(head[x + 4:x + 8], "big")
        if flags & 0x1:
            frames = int.from_bytes(head[x + 8:x + 12], "big")
            if frames > 0:
                return frames * spf / sr
    # 退化：按 CBR 比特率估算（无 Xing 头时）
    if size > start:
        return (size - start) * 8 / br
    return None


# ---------------------------------------------------------------- WAV
def _wav(head, size):
    if len(head) < 12 or head[:4] != b"RIFF" or head[8:12] != b"WAVE":
        return None
    n = len(head)
    pos = 12
    rate = ch = bits = 0
    byte_rate = 0
    data_bytes = None
    while pos + 8 <= n:
        cid = head[pos:pos + 4]
        csz = struct.unpack_from("<I", head, pos + 4)[0]
        body = pos + 8
        if cid == b"fmt " and body + 16 <= n:
            _af, ch, rate, byte_rate, _ba, bits = struct.unpack_from("<HHIIHH", head, body)
        elif cid == b"data":
            # csz==0 表示 data 一直延伸到文件尾
            data_bytes = csz if csz > 0 else max(0, size - body)
            break
        if csz == 0:
            break
        pos = body + csz
    if not byte_rate and rate and ch and bits:
        byte_rate = rate * ch * bits // 8
    if not byte_rate or data_bytes is None:
        return None
    return data_bytes / byte_rate


# ---------------------------------------------------------------- FLAC
def _flac(head, _size):
    if len(head) < 42 or head[:4] != b"fLaC":
        return None
    # 第一个 metadata block 必须是 STREAMINFO（type 0）
    if (head[4] & 0x7F) != 0:
        return None
    # block data 从 8 开始；sample_rate(20) channels(3) bps(5) total_samples(36) 在 [18:26]
    v = int.from_bytes(head[18:26], "big")
    sample_rate = (v >> 44) & 0xFFFFF
    total_samples = v & 0xFFFFFFFFF
    if not sample_rate or not total_samples:
        return None
    return total_samples / sample_rate


# ---------------------------------------------------------------- 入口
def probe_duration(head, tail, size, name=""):
    """从采样字节里判断媒体时长，返回秒（float）；无法判断返回 None。

    :param head: 文件头部采样字节
    :param tail: 文件尾部采样字节
    :param size: 文件总字节数（MP3 的 CBR 估算需要）
    :param name: 文件名，仅用于取扩展名做提示
    """
    ext = name.rsplit(".", 1)[-1].lower() if name and "." in name else ""

    # 1) 扩展名明确时优先走对应解析器（最准，也避免魔数误判）
    if ext in ("wav", "wave"):
        d = _wav(head, size)
        if d:
            return d
    elif ext == "flac":
        d = _flac(head, size)
        if d:
            return d
    elif ext == "mp3":
        d = _mp3(head, size)
        if d:
            return d
    elif ext in ("mp4", "m4a", "m4b", "m4v", "mov", "3gp", "3g2"):
        d = _mp4(head, tail)
        if d:
            return d

    # 2) 魔数嗅探（扩展名缺失或不靠谱时兜底）
    if head[:4] == b"fLaC":
        d = _flac(head, size)
        if d:
            return d
    if head[:4] == b"RIFF" and head[8:12] == b"WAVE":
        d = _wav(head, size)
        if d:
            return d
    if len(head) >= 8 and head[4:8] in _FTYP_LIKE:
        d = _mp4(head, tail)
        if d:
            return d
    if head[:3] == b"ID3" or (
        len(head) >= 2 and head[0] == 0xFF and (head[1] & 0xE0) == 0xE0
    ):
        d = _mp3(head, size)
        if d:
            return d
    return None

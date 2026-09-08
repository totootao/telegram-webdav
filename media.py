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


# ---------------------------------------------------------------- MKV / WebM (EBML)
# EBML 元素 ID（顶层/常用）
_EBML_SEGMENT = 0x18538067
_EBML_INFO = 0x1549A966
_EBML_TIMESTAMPSCALE = 0x2AD7B1
_EBML_DURATION = 0x4489
_EBML_CLUSTER = 0x1F43B675
_EBML_MAGIC = b"\x1a\x45\xdf\xa3"  # EBML header 固定以这 4 字节开头


def _vint(b, pos, keep_marker):
    """读 EBML 变长整数(VINT)，返回 (值, 占用字节数)；失败返回 (None, 0)。

    VINT 规则：第一个字节的前导零个数 + 1 = 总长度；那个"1"是长度标记位。
    读 ID 时保留标记位（ID 本身含它），读 Size 时要去掉。
    """
    if pos >= len(b):
        return None, 0
    b0 = b[pos]
    if b0 == 0:
        return None, 0
    n = 0
    while not (b0 & (0x80 >> n)):
        n += 1
        if n > 7:
            return None, 0
    length = n + 1
    if pos + length > len(b):
        return None, 0
    if keep_marker:
        return int.from_bytes(b[pos:pos + length], "big"), length
    val = b0 & (0xFF >> (n + 1))
    for i in range(1, length):
        val = (val << 8) | b[pos + i]
    return val, length


def _ebml_iter(b, start, end):
    """迭代 [start, end) 区间内的 EBML 元素，yield (id, payload_start, payload_end)。

    Size 为"未知长度"（数据位全 1，直播流常见）时，把余下区间整体当作该元素负载——
    这样 Segment 未知长度时仍能进去找 Info。
    """
    pos = start
    while pos < end:
        eid, ln = _vint(b, pos, True)
        if eid is None:
            return
        pos += ln
        size, sn = _vint(b, pos, False)
        if size is None:
            return
        pos += sn
        if size == (1 << (7 * sn)) - 1:  # 未知长度
            yield eid, pos, end
            return
        if pos + size > end:
            yield eid, pos, end
            return
        yield eid, pos, pos + size
        pos += size


def _mkv(head, tail):
    """Matroska / WebM：取 Segment→Info 里的 TimestampScale 与 Duration。

    Duration 的单位是 TimestampScale（不是秒），需换算：
        秒 = Duration × TimestampScale / 1e9
    """
    data = None
    for cand in (head, tail):
        if len(cand) >= 4 and cand[:4] == _EBML_MAGIC:
            data = cand
            break
    if data is None:
        return None
    seg = None
    for eid, ps, pe in _ebml_iter(data, 0, len(data)):
        if eid == _EBML_SEGMENT:
            seg = (ps, pe)
            break
    if seg is None:
        return None
    scale = None
    dur = None
    for eid, ps, pe in _ebml_iter(data, seg[0], seg[1]):
        if eid == _EBML_INFO:
            for ieid, ips, ipe in _ebml_iter(data, ps, pe):
                if ieid == _EBML_TIMESTAMPSCALE and scale is None:
                    scale = int.from_bytes(data[ips:ipe], "big")
                elif ieid == _EBML_DURATION and dur is None:
                    n = ipe - ips
                    if n == 4:
                        dur = struct.unpack(">f", data[ips:ipe])[0]
                    elif n == 8:
                        dur = struct.unpack(">d", data[ips:ipe])[0]
            break
        if eid == _EBML_CLUSTER:  # 已进 Cluster，Info 不会在它后面
            break
    if not dur or dur <= 0:
        return None
    return dur * (scale or 1000000) / 1e9


# ---------------------------------------------------------------- OGG
_OGG_MAGIC = b"OggS"


def _ogg_page(b, pos):
    """解析 pos 处的 Ogg 页，返回 (granule, header_type, page_end) 或 None。"""
    if pos + 27 > len(b) or b[pos:pos + 4] != _OGG_MAGIC or b[pos + 4] != 0:
        return None
    granule = int.from_bytes(b[pos + 6:pos + 14], "little", signed=True)
    htype = b[pos + 5]
    nseg = b[pos + 26]
    if pos + 27 + nseg > len(b):
        return None
    # 页负载总长 = segment_table 各项之和
    page_end = pos + 27 + nseg + sum(b[pos + 27:pos + 27 + nseg])
    return granule, htype, page_end


def _ogg_codec(head):
    """从首页的 ID header 认 codec，返回 (granule_rate, pre_skip)；不认识返回 None。"""
    p = _ogg_page(head, 0)
    if p is None:
        return None
    _granule, _htype, _pe = p
    dstart = 27 + head[26]
    d = head[dstart:dstart + 64]
    # Opus：granule 恒定以 48kHz 计，且要减掉 pre_skip
    if d.startswith(b"OpusHead") and len(d) >= 16:
        return 48000.0, int.from_bytes(d[10:12], "little")
    # Vorbis：granule 是 PCM 采样数，采样率在 ID header 的 12:16
    if len(d) >= 16 and d[0] == 0x01 and d[1:7] == b"vorbis":
        sr = int.from_bytes(d[12:16], "little")
        if sr > 0:
            return float(sr), 0
    # Theora(视频) 的 granule 编码了帧号与关键帧偏移，暂不支持
    return None


def _ogg(head, tail):
    """OGG：首页认 codec 取 granule 速率，尾页取 granule_position 换算时长。"""
    info = _ogg_codec(head)
    if info is None:
        return None
    rate, pre_skip = info
    best_eos = best_max = None
    n = len(tail)
    pos = 0
    while True:
        i = tail.find(_OGG_MAGIC, pos)
        if i < 0:
            break
        p = _ogg_page(tail, i)
        if p is not None:
            granule, htype, pe = p
            # 页必须完整落在采样内，且 granule 有效，才算可信候选
            if pe <= n and granule > 0:
                if best_max is None or granule > best_max:
                    best_max = granule
                if htype & 0x04:  # EOS：真正的最后一页，最可信
                    if best_eos is None or granule > best_eos:
                        best_eos = granule
        pos = i + 1
    granule = best_eos if best_eos is not None else best_max
    if not granule:
        return None
    dur = (granule - pre_skip) / rate
    return dur if dur > 0 else None


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
    elif ext in ("mkv", "webm"):
        d = _mkv(head, tail)
        if d:
            return d
    elif ext in ("ogg", "oga", "opus", "ogv", "ogx", "spx"):
        d = _ogg(head, tail)
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
    if head[:4] == _EBML_MAGIC:
        d = _mkv(head, tail)
        if d:
            return d
    if head[:4] == _OGG_MAGIC:
        d = _ogg(head, tail)
        if d:
            return d
    if head[:3] == b"ID3" or (
        len(head) >= 2 and head[0] == 0xFF and (head[1] & 0xE0) == 0xE0
    ):
        d = _mp3(head, size)
        if d:
            return d
    return None

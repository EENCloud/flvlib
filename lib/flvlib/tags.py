import os
import struct
import logging

from primitives import *
from constants import *
from astypes import MalformedFLV
from astypes import get_script_data_variable, make_script_data_variable
from bitstring import BitStream

log = logging.getLogger('flvlib.tags')

STRICT_PARSING = False

frame_num_width = 4;

def strict_parser():
    return globals()['STRICT_PARSING']


class EndOfTags(Exception):
    pass


def ensure(value, expected, error_msg):
    if value == expected:
        return

    if strict_parser():
        raise MalformedFLV(error_msg)
    else:
        log.warning(error_msg)


class Tag(object):

    def __init__(self, parent_flv, f):
        self.f = f
        self.parent_flv = parent_flv
        self.offset = None
        self.size = None
        self.stream_id = 0
        self.timestamp = None
        self.type = 0

    def write(self, outfile):
        outfile.write(make_ui8(self.type))
        outfile.write(make_ui24(self.size))
        outfile.write(make_si32_extended(self.timestamp))
        outfile.write(make_ui24(self.stream_id))
        self.write_tag_content(outfile)
        outfile.write(make_ui32(self.previous_tag_size))


    def parse(self):
        f = self.f

        self.offset = f.tell() - 1

        # DataSize
        self.size = get_ui24(f)

        # Timestamp + TimestampExtended
        self.timestamp = get_si32_extended(f)

        if self.timestamp < 0:
            log.warning("The tag at offset 0x%08X has negative timestamp: %d",
                        self.offset, self.timestamp)

        # StreamID
        stream_id = get_ui24(f)
        ensure(stream_id, 0, "StreamID non zero: 0x%06X" % stream_id)

        # The rest gets parsed in the subclass, it should move f to the
        # correct position to read PreviousTagSize
        self.parse_tag_content()

        self.previous_tag_size = get_ui32(f)
        ensure(self.previous_tag_size, self.size + 11,
               "PreviousTagSize of %d (0x%08X) "
               "not equal to actual tag size of %d (0x%08X)" %
               (self.previous_tag_size, self.previous_tag_size,
                self.size + 11, self.size + 11))

    def parse_tag_content(self):
        # By default just seek past the tag content
        self.f.seek(self.size, os.SEEK_CUR)

class AudioTag(Tag):

    def __init__(self, parent_flv, f):
        Tag.__init__(self, parent_flv, f)
        self.sound_format = None
        self.sound_rate = None
        self.sound_size = None
        self.sound_type = None
        self.aac_packet_type = None  # always None for non-AAC tags

    def parse_tag_content(self):
        f = self.f

        sound_flags = get_ui8(f)
        read_bytes = 1

        self.sound_format = (sound_flags & 0xF0) >> 4
        self.sound_rate = (sound_flags & 0xC) >> 2
        self.sound_size = (sound_flags & 0x2) >> 1
        self.sound_type = sound_flags & 0x1

        if self.sound_format == SOUND_FORMAT_AAC:
            # AAC packets can be sequence headers or raw data.
            # The former contain codec information needed by the decoder to be
            # able to interpret the rest of the data.
            self.aac_packet_type = get_ui8(f)
            read_bytes += 1
            # AAC always has sampling rate of 44 kHz
            ensure(self.sound_rate, SOUND_RATE_44_KHZ,
                   "AAC sound format with incorrect sound rate: %d" %
                   self.sound_rate)
            # AAC is always stereo
            ensure(self.sound_type, SOUND_TYPE_STEREO,
                   "AAC sound format with incorrect sound type: %d" %
                   self.sound_type)

        self.frame_offset = self.offset + 12

        if strict_parser():
            try:
                sound_format_to_string[self.sound_format]
            except KeyError:
                raise MalformedFLV("Invalid sound format: %d",
                                   self.sound_format)
            try:
                (self.aac_packet_type and
                 aac_packet_type_to_string[self.aac_packet_type])
            except KeyError:
                raise MalformedFLV("Invalid AAC packet type: %d",
                                   self.aac_packet_type)

        f.seek(self.size - read_bytes, os.SEEK_CUR)

    def __repr__(self):
        if self.offset is None:
            return "<AudioTag unparsed>"
        elif self.aac_packet_type is None:
            return ("<AudioTag at offset 0x%08X, time %d, size %d, %s>" %
                    (self.offset, self.timestamp, self.size,
                     sound_format_to_string.get(self.sound_format, '?')))
        else:
            return ("<AudioTag at offset 0x%08X, time %d, size %d, %s, %s>" %
                    (self.offset, self.timestamp, self.size,
                     sound_format_to_string.get(self.sound_format, '?'),
                     aac_packet_type_to_string.get(self.aac_packet_type, '?')))





class AVCDecoderConfigurationRecord():
    def __init__(self, tag, f):
        self.f = f
        self.configurationVersion = 0
        self.avcProfileIndication = 0
        self.profileCompatibility = 0
        self.avcLevelIndication = 0
        self.reserved = 63
        self.RLELength = 0
        self.reserved2 = 7
        self.numSPS = 0
        self.sps = []
        self.numPPS = 0
        self.pps = []

    def parse_tag_content(self):
        self.configurationVersion = get_ui8(self.f)
        self.avcProfileIndication = get_ui8(self.f)
        self.profileCompatibility = get_ui8(self.f)
        self.avcLevelIndication = get_ui8(self.f)
        temp = get_ui8(self.f)
        self.reserved = (temp >> 2) & 0x3F
        self.RLELength = (temp & 0x3)
        temp = get_ui8(self.f)
        self.reserved2 = temp >> 5
        self.numSPS = temp & 0x1F
        for i in xrange(self.numSPS):
            sps = SPS(self, self.f)
            sps.parse_tag_content(size_width=2)
            self.sps.append(sps)
        self.numPPS = get_ui8(self.f)
        for i in xrange(self.numPPS):
            pps = NALU(self, self.f)
            pps.parse_tag_content(size_width=2)
            self.pps.append(pps)

    def write_tag_content(self, outfile):
        outfile.write(make_ui8(self.configurationVersion))
        outfile.write(make_ui8(self.avcProfileIndication))
        outfile.write(make_ui8(self.profileCompatibility))
        outfile.write(make_ui8(self.avcLevelIndication))
        temp = (self.reserved << 2) | (self.RLELength & 0x3)
        outfile.write(make_ui8(temp))
        temp = self.reserved2 << 5 | (len(self.sps) & 0x3)
        outfile.write(make_ui8(temp))
        for sps in self.sps:
            sps.write_tag_content(outfile, size_width=2)
        outfile.write(make_ui8(len(self.pps)))
        for pps in self.pps:
            pps.write_tag_content(outfile, size_width=2)

    def __repr__(self):
        return "AVCDecoderConfigurationRecord: confVersion: {} profileIndication: {} \
        profileComapt: {} avcLevel: {} RLELength: {} numSPS: {} numPPS: {}".format(self.configurationVersion,
                                                                                   self.avcProfileIndication,
                                                                                   self.profileCompatibility,
                                                                                   self.avcLevelIndication,
                                                                                   self.RLELength,
                                                                                   self.numSPS,
                                                                                   self.numPPS)


class NALU(Tag):
    def __init__(self, tag, f):
        self.f = f;
        self.size = 0;
        self.type = 0;
        self.data = 0;
        self.offset = 0;

    def write_tag_content(self, outfile, size_width=4):
        if (size_width == 1):
            outfile.write(make_ui8(self.size))
        elif (size_width == 2):
            outfile.write(make_ui16(self.size))
        elif (size_width == 3):
            outfile.write(make_ui24(self.size))
        elif (size_width == 4):
            outfile.write(make_ui32(self.size))

        outfile.write(self.data)

    def parse_tag_content(self, size_width=4):
        self.offset = self.f.tell()
        if size_width == 1:
            self.size = get_ui8(self.f)
        elif size_width == 2:
            self.size = get_ui16(self.f)
        elif size_width == 3:
            self.size = get_ui24(self.f)
        elif size_width == 4:
            self.size = get_ui32(self.f)

        self.data = self.f.read(self.size)
        s = BitStream('0x' + self.data.encode('hex'))
        self.forbidden_bit = s.read('uint:1')
        self.nal_ref_idc = s.read('uint:2')
        self.nal_unit_type = s.read('uint:5')
        self.type = self.nal_unit_type
        #self.type = int(struct.unpack('B', self.data[0])[0]) & 31
        if (self.type == 1 or self.type == 5):
            #s = BitStream('0x' + self.data[1:].encode('hex'))
            self.first_mb_in_slice = s.read('ue')
            self.slice_type = s.read('ue')
            self.pic_parameter_set_id = s.read('ue')
            self.frame_num = s.read('uint:{}'.format(frame_num_width))

    def __repr__(self):
        if self.type == 1:
            return "NALU P-Frame {} {} #{}".format(self.size, self.offset, self.frame_num)
        if self.type == 5:
            return "NALU I-Frame {} {} #{}".format(self.size, self.offset, self.frame_num)
        if self.type == 7:
            return "NALU SPS {} {}".format(self.size, self.offset)
        if self.type == 8:
            return "NALU PPS {} {}".format(self.size, self.offset)
        if self.type == 9:
            return "NALU AUD {} {}".format(self.size, self.offset)

        return "NALU {} {}".format(self.type, self.size, self.offset)


CROP_LEFT = 0
CROP_RIGHT = 1
CROP_TOP = 2
CROP_BOTTOM = 3
class SPS(NALU):
    chroma_profiles = [100, 110, 122, 244, 44, 83, 86, 118, 128, 134, 138, 139]
    parsed = False
    forbidden_bit = -1
    nal_ref_idc = -1
    nal_unit_type = -1
    profile_idc = -1
    constraint_flags = []
    reserved_bits = -1
    level_idc = -1
    seq_parameter_set_id = -1
    log2_max_frame_num_minus4 = -1
    pic_order_cnt_type = -1
    log2_max_pic_order_cnt_lsb_minus4 = -1
    num_ref_frames = -1
    gaps_in_frame_num_value_allowed_flag = -1
    frame_mbs_only_flag = -1
    direct_8x8_inference_flag = -1
    frame_cropping_flag = -1
    vui_prameters_present_flag = -1
    rbsp_stop_one_bit = -1
    chroma_format_idc = -1
    separate_color_plane_flag = -1
    bit_depth_luma_minus8 = -1
    bit_depth_chroma_minus8 = -1
    qpprime_y_zero_transform_bypass_flag = -1
    seq_scaling_matrix_present_flag = -1
    sequence_scaling_list = []
    delta_pic_order_always_zero_flag = -1
    offset_for_non_ref_pic = -1
    offset_for_top_to_bottom_field = -1
    num_ref_frames_in_pic_order_cnt_cycle = -1
    offsets_for_ref_frame = -1
    max_num_ref_frames = -1
    pic_width_in_mbs_minus1 = -1
    pic_height_in_map_units_minus1 = -1
    mb_adaptive_frame_field_flag = -1
    frame_crop_offsets = []
    vui_parameters_present_flag = -1

    def __init__(self, tag, f):
        NALU.__init__(self, tag, f)

    def parse_sps_data(self):
        s = BitStream('0x' + self.data.encode('hex'))
        self.forbidden_bit = s.read('uint:1')
        self.nal_ref_idc = s.read('uint:2')
        self.nal_unit_type = s.read('uint:5')
        self.profile_idc = s.read('uint:8')
        self.constraint_flags = s.readlist('4*uint:1')
        self.reserved_bits = s.read('uint:4')
        self.level_idc = s.read('uint:8')
        self.seq_parameter_set_id = s.read('ue')
        if self.profile_idc in self.chroma_profiles:
            self.chroma_format_idc = s.read('ue')
            if self.chroma_format_idc == 3:
                self.separate_color_plane_flag = s.read('uint:1')
            self.bit_depth_luma_minus8 = s.read('ue')
            self.bit_depth_chroma_minus8 = s.read('ue')
            self.qpprime_y_zero_transform_bypass_flag = s.read('uint:1')
            self.seq_scaling_matrix_present_flag = s.read('uint:1')
            if self.seq_scaling_matrix_present_flag:
                seq_scaling_list_present = s.read('uint:1')
                scaling_list_count = 12 if self.chroma_format_idc == 3 else 8
                for i in xrange(scaling_list_count):
                    if seq_scaling_list_present:
                        last_scale = 8
                        next_scale = 8
                        list_size = 16 if i < 6 else 64
                        for j in xrange(list_size):
                            delta_scale = s.read('ue')

        self.log2_max_frame_num_minus4 = s.read('ue')
        self.log2_max_frame_num = self.log2_max_frame_num_minus4 + 4
        frame_num_width = self.log2_max_frame_num;
        self.pic_order_cnt_type = s.read('ue')
        if self.pic_order_cnt_type == 0:
            self.log2_max_pic_order_cnt_lsb_minus4 = s.read('ue')
        elif self.pic_order_cnt_type == 1:
            self.delta_pic_order_always_zero_flag = s.read('uint:1')
            self.offset_for_non_ref_pic = s.read('se')
            self.offset_for_top_to_bottom_field = s.read('se')
            self.num_ref_frames_in_pic_order_cnt_cycle = s.read('ue')
            self.offsets_for_ref_frame = s.readlist('4*')

        self.max_num_ref_frames = s.read('ue')
        self.gaps_in_frame_num_value_allowed_flag = s.read('uint:1')
        self.pic_width_in_mbs_minus1 = s.read('ue')
        self.pic_height_in_map_units_minus1 = s.read('ue')
        self.frame_mbs_only_flag = s.read('uint:1')
        if self.frame_mbs_only_flag == 0:
            self.mb_adaptive_frame_field_flag = s.read('uint:1')
        self.direct_8x8_inference_flag = s.read('uint:1')
        self.frame_cropping_flag = s.read('uint:1')
        self.frame_crop_offsets = s.readlist('4*ue')
        self.vui_parameters_present_flag = s.read('uint:1')
        self.width = 16 * (self.pic_width_in_mbs_minus1 + 1)
        self.height = 16 * (2 - self.frame_mbs_only_flag) * (self.pic_height_in_map_units_minus1 + 1)

        if self.separate_color_plane_flag or self.chroma_format_idc == 0:
            self.frame_crop_offsets[CROP_BOTTOM] = self.frame_crop_offsets[CROP_BOTTOM] * ( 2 - self.frame_mbs_only_flag)
            self.frame_crop_offsets[CROP_TOP] = self.frame_crop_offsets[CROP_TOP] * (2 - self.frame_mbs_only_flag)
        elif not self.separate_color_plane_flag and self.chroma_format_idc > 0:
            if self.chroma_format_idc == 1 or self.chroma_format_idc == 2:
                self.frame_crop_offsets[CROP_LEFT] = self.frame_crop_offsets[CROP_LEFT] * 2
                self.frame_crop_offsets[CROP_RIGHT] = self.frame_crop_offsets[CROP_RIGHT] * 2

            if self.chroma_format_idc == 1:
                self.frame_crop_offsets[CROP_TOP] = self.frame_crop_offsets[CROP_TOP] * 2
                self.frame_crop_offsets[CROP_BOTTOM] = self.frame_crop_offsets[CROP_BOTTOM] * 2

        self.width = self.width - (self.frame_crop_offsets[CROP_LEFT] + self.frame_crop_offsets[CROP_RIGHT])
        self.height = self.height - (self.frame_crop_offsets[CROP_TOP] + self.frame_crop_offsets[CROP_BOTTOM])

class VideoTag(Tag):

    def __init__(self, parent_flv, f):
        Tag.__init__(self, parent_flv, f)
        self.frame_type = None
        self.codec_id = None
        self.nalus = []
        self.h264_packet_type = None # Always None for non-H.264 tags

    def write_tag_content(self, outfile):
        temp = (self.frame_type << 4) | self.codec_id
        outfile.write(make_ui8(temp))
        if self.codec_id == CODEC_ID_H264:
            outfile.write(make_ui8(self.h264_packet_type))
            outfile.write(make_ui24(self.composition_time))
            if (self.h264_packet_type == 1):
                for nalu in self.nalus:
                    nalu.write_tag_content(outfile)
            else:
                self.configurationRecord.write_tag_content(outfile)



    def parse_tag_content(self):
        f = self.f

        video_flags = get_ui8(f)
        read_bytes = 1

        self.frame_type = (video_flags & 0xF0) >> 4
        self.codec_id = video_flags & 0xF

        if self.codec_id == CODEC_ID_H264:
            # H.264 packets can be sequence headers, NAL units or sequence
            # ends.
            self.h264_packet_type = get_ui8(f)
            read_bytes += 1

            self.composition_time = get_ui24(f)
            read_bytes += 3

            self.avc_header_offset = self.offset + 16
            self.frame_offset = self.avc_header_offset
            self.data_size = self.size - 5
            if (self.h264_packet_type == 1):
                self.nalus = []
                if self.parent_flv.encrypted:
                    f.seek(self.data_size, os.SEEK_CUR)
                else:
                    bytesRead = 0
                    while bytesRead < self.data_size - 12:
                        nal = NALU(self, f)
                        nal.parse_tag_content()
                        bytesRead += nal.size
                        read_bytes += nal.size
                        self.nalus.append(nal)
            else:
                self.configurationRecord = AVCDecoderConfigurationRecord(self, self.f)
                self.configurationRecord.parse_tag_content()


        if strict_parser():
            try:
                frame_type_to_string[self.frame_type]
            except KeyError:
                raise MalformedFLV("Invalid frame type: %d", self.frame_type)
            try:
                codec_id_to_string[self.codec_id]
            except KeyError:
                raise MalformedFLV("Invalid codec ID: %d", self.codec_id)
            try:
                (self.h264_packet_type and
                 h264_packet_type_to_string[self.h264_packet_type])
            except KeyError:
                raise MalformedFLV("Invalid H.264 packet type: %d",
                                   self.h264_packet_type)

    def __repr__(self):
        if self.offset is None:
            return "<VideoTag unparsed>"
        elif self.h264_packet_type is None:
            return ("<VideoTag at offset 0x%08X, time %d, size %d, %s (%s)>" %
                    (self.offset, self.timestamp, self.size,
                     codec_id_to_string.get(self.codec_id, '?'),
                     frame_type_to_string.get(self.frame_type, '?')))
        else:
            return ("<VideoTag at offset 0x%08X, "
                    "time %d, size %d, %s (%s), %s>" %
                    (self.offset, self.timestamp, self.size,
                     codec_id_to_string.get(self.codec_id, '?'),
                     frame_type_to_string.get(self.frame_type, '?'),
                     self.nalus))


class ScriptTag(Tag):

    def __init__(self, parent_flv, f):
        Tag.__init__(self, parent_flv, f)
        self.name = None
        self.variable = None

    def parse_tag_content(self):
        f = self.f
        self.data = f.read(self.size)


    def write_tag_content(self, outfile):
        outfile.write(self.data)

    def __repr__(self):
        if self.offset is None:
            return "<ScriptTag unparsed>"
        else:
            return ("<ScriptTag %s at offset 0x%08X, time %d, size %d>" %
                    (self.name, self.offset, self.timestamp, self.size))


class ScriptAMF3Tag(Tag):

    def __repr__(self):
        if self.offset is None:
            return "<ScriptAMF3Tag unparsed>"
        else:
            return ("<ScriptAMF3Tag at offset 0x%08X, time %d, size %d>" %
                    (self.offset, self.timestamp, self.size))


tag_to_class = {
    TAG_TYPE_AUDIO: AudioTag,
    TAG_TYPE_VIDEO: VideoTag,
    TAG_TYPE_SCRIPT_AMF3: ScriptAMF3Tag,
    TAG_TYPE_SCRIPT: ScriptTag,
}


class FLV(object):

    def __init__(self, f, **kwargs):
        self.f = f
        self.version = None
        self.has_audio = None
        self.has_video = None
        self.encrypted = kwargs.get('encrypted', False)
        self.tags = []

    def parse_header(self):
        f = self.f
        f.seek(0)

        # FLV header
        header = f.read(3)
        if len(header) < 3:
            raise MalformedFLV("The file is shorter than 3 bytes")

        # Do this irrelevant of STRICT_PARSING, to catch bogus files
        if header != "FLV":
            raise MalformedFLV("File signature is incorrect: 0x%X 0x%X 0x%X" %
                               struct.unpack("3B", header))

        # File version
        self.version = get_ui8(f)
        log.debug("File version is %d", self.version)

        # TypeFlags
        flags = get_ui8(f)

        ensure(flags & 0xF8, 0,
               "First TypeFlagsReserved field non zero: 0x%X" % (flags & 0xF8))
        ensure(flags & 0x2, 0,
               "Second TypeFlagsReserved field non zero: 0x%X" % (flags & 0x2))

        self.has_audio = False
        self.has_video = False
        if flags & 0x4:
            self.has_audio = True
        if flags & 0x1:
            self.has_video = True
        log.debug("File %s audio",
                  (self.has_audio and "has") or "does not have")
        log.debug("File %s video",
                  (self.has_video and "has") or "does not have")

        header_size = get_ui32(f)
        log.debug("Header size is %d bytes", header_size)

        f.seek(header_size)

        tag_0_size = get_ui32(f)
        ensure(tag_0_size, 0, "PreviousTagSize0 non zero: 0x%08X" % tag_0_size)

    def iter_tags(self):
        self.parse_header()
        try:
            while True:
                tag = self.get_next_tag()
                yield tag
        except EndOfTags:
            pass

    def read_tags(self):
        self.tags = list(self.iter_tags())

    def get_next_tag(self):
        f = self.f

        try:
            tag_type = get_ui8(f)
        except EndOfFile:
            raise EndOfTags

        tag_klass = self.tag_type_to_class(tag_type)
        tag = tag_klass(self, f)
        tag.type = tag_type
        tag.parse()

        return tag

    def tag_type_to_class(self, tag_type):
        try:
            return tag_to_class[tag_type]
        except KeyError:
            raise MalformedFLV("Invalid tag type: %d at offset {} (Size: {})", tag_type, self.f.tell())


def create_flv_tag(type, data, timestamp=0):
    tag_type = struct.pack("B", type)
    timestamp = make_si32_extended(timestamp)
    stream_id = make_ui24(0)

    data_size = len(data)
    tag_size = data_size + 11

    return ''.join([tag_type, make_ui24(data_size), timestamp, stream_id,
                    data, make_ui32(tag_size)])


def create_script_tag(name, data, timestamp=0):
    payload = make_ui8(2) + make_script_data_variable(name, data)
    return create_flv_tag(TAG_TYPE_SCRIPT, payload, timestamp)


def create_flv_header(has_audio=True, has_video=True):
    type_flags = 0
    if has_video:
        type_flags = type_flags | 0x1
    if has_audio:
        type_flags = type_flags | 0x4
    return ''.join(['FLV', make_ui8(1), make_ui8(type_flags), make_ui32(9),
                    make_ui32(0)])

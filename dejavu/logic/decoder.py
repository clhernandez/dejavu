import fnmatch
import os
import io
from hashlib import sha1
from typing import List, Tuple
from hashlib import md5

import numpy as np
from pydub import AudioSegment
from pydub.utils import audioop

from dejavu.third_party import wavio


def generate_fingerprint_md5(fingerprints):
    # Convert the list of fingerprints into a string
    fingerprint_str = '|'.join([f'{fp[0]}-{fp[1]}' for fp in fingerprints])
    
    # Generate the MD5 hash of the fingerprint string
    md5_hash = md5(fingerprint_str.encode()).hexdigest()
    
    return md5_hash

def unique_hash(file_path: str, block_size: int = 2**20) -> str:
    """ Small function to generate a hash to uniquely generate
    a file. Inspired by MD5 version here:
    http://stackoverflow.com/a/1131255/712997

    Works with large files.

    :param file_path: path to file.
    :param block_size: read block size.
    :return: a hash in an hexagesimal string form.
    """
    s = sha1()
    with open(file_path, "rb") as f:
        while True:
            buf = f.read(block_size)
            if not buf:
                break
            s.update(buf)
    return s.hexdigest().upper()

def unique_bytes_hash(data: bytes, block_size: int = 2**20) -> str:
    """ Small function to generate a hash to uniquely generate
    a byte array. Inspired by MD5 version here:
    http://stackoverflow.com/a/1131255/712997

    Works with large byte arrays.

    :param data: byte array.
    :param block_size: read block size.
    :return: a hash in an hexagesimal string form.
    """
    s = sha1()
    buffer = memoryview(data)
    index = 0
    while index < len(buffer):
        buf = buffer[index:index+block_size]
        s.update(buf)
        index += block_size
    return s.hexdigest().upper()


def find_files(path: str, extensions: List[str]) -> List[Tuple[str, str]]:
    """
    Get all files that meet the specified extensions.

    :param path: path to a directory with audio files.
    :param extensions: file extensions to look for.
    :return: a list of tuples with file name and its extension.
    """
    # Allow both with ".mp3" and without "mp3" to be used for extensions
    extensions = [e.replace(".", "") for e in extensions]

    results = []
    for dirpath, dirnames, files in os.walk(path):
        for extension in extensions:
            for f in fnmatch.filter(files, f"*.{extension}"):
                p = os.path.join(dirpath, f)
                results.append((p, extension))
    return results


def read(file_name: str, limit: int = None) -> Tuple[List[List[int]], int, str]:
    """
    Reads any file supported by pydub (ffmpeg) and returns the data contained
    within. If file reading fails due to input being a 24-bit wav file,
    wavio is used as a backup.

    Can be optionally limited to a certain amount of seconds from the start
    of the file by specifying the `limit` parameter. This is the amount of
    seconds from the start of the file.

    :param file_name: file to be read.
    :param limit: number of seconds to limit.
    :return: tuple list of (channels, sample_rate, content_file_hash).
    """
    # pydub does not support 24-bit wav files, use wavio when this occurs
    try:
        audiofile = AudioSegment.from_file(file_name)

        if limit:
            audiofile = audiofile[:limit * 1000]

        data = np.fromstring(audiofile.raw_data, np.int16)

        channels = []
        for chn in range(audiofile.channels):
            channels.append(data[chn::audiofile.channels])

        audiofile.frame_rate
    except audioop.error:
        _, _, audiofile = wavio.readwav(file_name)

        if limit:
            audiofile = audiofile[:limit * 1000]

        audiofile = audiofile.T
        audiofile = audiofile.astype(np.int16)

        channels = []
        for chn in audiofile:
            channels.append(chn)

    return channels, audiofile.frame_rate, unique_hash(file_name)


def get_audio_name_from_path(file_path: str) -> str:
    """
    Extracts song name from a file path.

    :param file_path: path to an audio file.
    :return: file name
    """
    return os.path.splitext(os.path.basename(file_path))[0]

def read_from_buffer(buffer: bytes, frame_rate: int, sample_width: int, audio_channels: int, limit: int = None) -> Tuple[List[List[int]], int, str]:
    """
    Reads audio data from a buffer and returns the data contained within.
    If the buffer contains a 24-bit wav file, wavio is used as a backup.

    Can be optionally limited to a certain amount of seconds from the start
    of the audio by specifying the `limit` parameter. This is the amount of
    seconds from the start of the audio.

    :param buffer: audio data buffer.
    :param limit: number of seconds to limit.
    :return: tuple list of (channels, sample_rate, content_file_hash).
    """
    try:
        audiofile = AudioSegment.from_raw(file=io.BytesIO(buffer), sample_width=sample_width, frame_rate=frame_rate, channels=audio_channels)

        if limit:
            audiofile = audiofile[:limit * 1000]

        if len(audiofile.raw_data) % 2:
            temp_data = bytes(audiofile.raw_data)
            temp_data = temp_data + b'\x00'
            audiofile = AudioSegment.from_raw(file=io.BytesIO(temp_data), sample_width=sample_width, frame_rate=frame_rate, channels=audio_channels)
        
        data = np.frombuffer(audiofile.raw_data, np.int16)

        channels = []
        for chn in range(audiofile.channels):
            channels.append(data[chn::audiofile.channels])

        audiofile.frame_rate
    except audioop.error:
        _, _, audiofile = wavio.readwav(buffer)

        if limit:
            audiofile = audiofile[:limit * 1000]

        audiofile = audiofile.T
        audiofile = audiofile.astype(np.int16)

        channels = []
        for chn in audiofile:
            channels.append(chn)

    return channels, audiofile.frame_rate, unique_bytes_hash(buffer)
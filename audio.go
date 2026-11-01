// The binary audio frame of an audio session: a 2-byte big-endian stream number, then 16-bit little-endian
// mono PCM at the session's sample rate, a whole number of samples. The header keeps the samples aligned, so a
// reader can view them as 16-bit integers without copying.

package voiceagents

import (
	"encoding/binary"
	"errors"
)

// HeaderBytes is the size of the stream number before the samples.
const HeaderBytes = 2

// StreamOf is the stream a frame belongs to: the session whose stream it is, on this connection.
func StreamOf(frame []byte) int { return int(binary.BigEndian.Uint16(frame)) }

// SamplesOf is the samples of a frame, as 16-bit little-endian mono PCM.
func SamplesOf(frame []byte) []byte { return frame[HeaderBytes:] }

// AudioFrame is a frame carrying samples for stream.
func AudioFrame(stream int, samples []byte) ([]byte, error) {
	if len(samples)%2 != 0 {
		return nil, errors.New("audio is a whole number of 16-bit samples")
	}
	frame := make([]byte, HeaderBytes+len(samples))
	binary.BigEndian.PutUint16(frame, uint16(stream))
	copy(frame[HeaderBytes:], samples)
	return frame, nil
}

// BytesPerFrame is how many bytes of samples carry milliseconds of audio at sampleRate: 640 for 20 ms at 16000 Hz.
func BytesPerFrame(sampleRate, milliseconds int) int { return sampleRate * milliseconds / 1000 * 2 }

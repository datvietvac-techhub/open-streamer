package domain

import (
	"errors"
	"fmt"

	"github.com/datvietvac-techhub/open-streamer/pkg/protocol"
)

// IsMixerInput reports whether the given Input is a `mixer://` reference.
func IsMixerInput(in Input) bool {
	return protocol.Detect(in.URL) == protocol.KindMixer
}

// MixerInputSpec parses a mixer:// input. Returns ("", "", false, error) for
// non-mixer or malformed URLs. Wraps protocol.MixerTargets so callers can
// preserve the user-facing message.
func MixerInputSpec(in Input) (videoCode, audioCode StreamCode, audioFailureContinue bool, err error) {
	if protocol.Detect(in.URL) != protocol.KindMixer {
		return "", "", false, fmt.Errorf("input %q is not a mixer:// URL", in.URL)
	}
	spec, err := protocol.MixerTargets(in.URL)
	if err != nil {
		return "", "", false, err
	}
	return StreamCode(spec.Video), StreamCode(spec.Audio), spec.AudioFailureContinue, nil
}

// MixerShapeError reports a mixer:// configuration that violates the v1
// constraints. Reason is API-surface text — handler returns it verbatim.
type MixerShapeError struct {
	StreamCode StreamCode
	Reason     string
}

func (e *MixerShapeError) Error() string {
	return fmt.Sprintf("stream %q: mixer:// shape: %s", e.StreamCode, e.Reason)
}

// IsMixerShapeError reports whether err is a *MixerShapeError.
func IsMixerShapeError(err error) bool {
	var t *MixerShapeError
	return errors.As(err, &t)
}

// ValidateMixerShape enforces mixer:// constraints on a single stream:
//
//  1. Self-mix (`mixer://A,X` or `mixer://X,A` inside stream A) is rejected —
//     a stream can't mix with itself.
//  2. mixer:// must be the SOLE input — fallback inputs are not supported in
//     v1 (failure semantics differ from regular failover; mixing across two
//     unrelated mixer specs is undefined).
//
// ABR upstreams ARE allowed:
//   - When the video upstream has an ABR ladder AND the downstream has no
//     own transcoder, the runtime mirrors the video ladder (N rungs out,
//     audio fanned-out across them). When the downstream HAS its own
//     transcoder, the runtime taps only the best rendition of each upstream
//     and feeds the encoder.
//   - Either upstream's audio is always single-source — the best rendition
//     of the audio upstream when it's ABR.
//
// `lookup` resolves upstream streams. Missing upstream is treated as
// "shape unknown" — the rule it enables is skipped, never failed. The
// MixerReader catches missing upstream at runtime as a hard error.
func ValidateMixerShape(s *Stream, lookup StreamLookup) error {
	if s == nil {
		return nil
	}

	mixerIdx := -1
	for i, in := range s.Inputs {
		if IsMixerInput(in) {
			mixerIdx = i
			break
		}
	}
	if mixerIdx < 0 {
		return nil // no mixer input — nothing to validate
	}

	// Rule 2: mixer must be the sole input.
	if len(s.Inputs) > 1 {
		return &MixerShapeError{
			StreamCode: s.Code,
			Reason:     "mixer:// must be the only input; fallback inputs are not supported in v1",
		}
	}

	video, audio, _, err := MixerInputSpec(s.Inputs[mixerIdx])
	if err != nil {
		// URL parse error is already reported by upstream URL validation
		// (handler.validateMixerConfigOn step 1) — silently skip the shape
		// rules that depend on a parsed target so we don't double-report.
		return nil //nolint:nilerr // intentional: URL grammar validator owns this error class
	}

	// Rule 1: self-mix.
	if video == s.Code || audio == s.Code {
		return &MixerShapeError{
			StreamCode: s.Code,
			Reason:     "self-mix not allowed (a stream cannot reference itself as video or audio source)",
		}
	}

	// Rules 3 (no ABR upstream) and 4 (no downstream transcoder) were
	// removed: ABR upstreams are now supported (mirror video ladder when
	// no downstream transcoder, single-tap-best when there is one), and a
	// downstream transcoder is now allowed (tap best video + best audio,
	// feed the encoder).

	return nil
}

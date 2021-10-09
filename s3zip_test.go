package s3zip

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
)

func TestNew(t *testing.T) {
	sess := session.New()

	t.Run("without config options", func(t *testing.T) {
		z := New(sess)

		if z.cfg != sess {
			t.Error("expected cfg to equal the passed config provider")
		}

		if z.concurrency != 1 {
			t.Errorf("expected default concurrency to be 1, got %d", z.concurrency)
		}
	})

	t.Run("with config options", func(t *testing.T) {
		z := New(sess,
			WithConcurrency(42),
		)

		if z.concurrency != 42 {
			t.Errorf("expected concurrency to be %d, got %d", 42, z.concurrency)
		}
	})
}

func TestWithConcurrency(t *testing.T) {
	z := New(session.New())

	for _, c := range []int{4, 2, 42} {
		t.Run(fmt.Sprintf("WithConcurrency(%d)", c), func(t *testing.T) {
			WithConcurrency(c)(&z)
			if z.concurrency != c {
				t.Errorf("expected to change concurrency to %d, got %d", c, z.concurrency)
			}
		})
	}
}

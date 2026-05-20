package memdb_test

// Build-smoke for the example mains. Examples are documentation, not
// libraries — their main() functions hardcode addresses, spin
// goroutines, and (in the cluster case) wait for SIGINT, so unit-testing
// their runtime behaviour would require carving each main into a
// testable helper. That refactor would harm the examples' role as
// copy-pastable documentation.
//
// What we DO guarantee: the examples compile against the current
// library API. A signature change in memdb.Open / Config / etc. that
// silently breaks them would otherwise only surface when someone tried
// to run the example by hand.

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestExamples_Build(t *testing.T) {
	// Rebuild every example as a smoke test. We discard the output
	// binaries (a temp dir cleaned up by t.TempDir).
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for _, ex := range []string{"cluster", "quickstart", "restapi"} {
		ex := ex
		t.Run(ex, func(t *testing.T) {
			t.Parallel()
			out := filepath.Join(t.TempDir(), ex)
			pkg := "github.com/voicetel/memdb/examples/" + ex
			cmd := exec.Command("go", "build", "-o", out, pkg)
			cmd.Dir = wd
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("go build %s: %v\n%s", pkg, err, output)
			}
		})
	}
}

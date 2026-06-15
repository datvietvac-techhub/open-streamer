package domain

import "testing"

func TestHookValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		hook     Hook
		fileRoot string
		wantErr  bool
	}{
		{"http ok", Hook{Type: HookTypeHTTP, Target: "https://x.test/h"}, "", false},
		{"http non-url", Hook{Type: HookTypeHTTP, Target: "x.test/h"}, "", true},
		{"http ftp scheme", Hook{Type: HookTypeHTTP, Target: "ftp://x.test/h"}, "", true},
		{"file abs, no root", Hook{Type: HookTypeFile, Target: "/var/log/a.log"}, "", false},
		{"file relative", Hook{Type: HookTypeFile, Target: "a.log"}, "", true},
		{"empty target", Hook{Type: HookTypeFile, Target: ""}, "", true},
		{"unknown type", Hook{Type: "ftp", Target: "/x"}, "", true},
		// Containment: with a root set, only paths inside it pass.
		{"file inside root", Hook{Type: HookTypeFile, Target: "/srv/hooks/a.log"}, "/srv/hooks", false},
		{"file outside root", Hook{Type: HookTypeFile, Target: "/etc/passwd"}, "/srv/hooks", true},
		{"file traversal escape", Hook{Type: HookTypeFile, Target: "/srv/hooks/../../etc/passwd"}, "/srv/hooks", true},
		{"file root prefix trick", Hook{Type: HookTypeFile, Target: "/srv/hooks-evil/a.log"}, "/srv/hooks", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.hook.Validate(c.fileRoot)
			if (err != nil) != c.wantErr {
				t.Errorf("Validate(%q) err=%v, wantErr=%v", c.fileRoot, err, c.wantErr)
			}
		})
	}
}

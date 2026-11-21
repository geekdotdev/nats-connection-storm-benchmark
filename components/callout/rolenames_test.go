// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"testing"
)

func TestSubjectsForRoles(t *testing.T) {
	cases := []struct {
		name     string
		roles    []string
		pub, sub []string
	}{
		{"no roles", nil, nil, nil},
		{"publish", []string{"app-feed-publish-orders"}, []string{"app.orders"}, nil},
		{"subscribe", []string{"app-feed-subscribe-alarms"}, nil, []string{"app.alarms"}},
		{"dashes become dots", []string{"app-feed-subscribe-alarms-high"}, nil, []string{"app.alarms.high"}},
		{"both directions, in order",
			[]string{"app-feed-subscribe-b", "app-feed-publish-a", "app-feed-publish-c"},
			[]string{"app.a", "app.c"}, []string{"app.b"}},
		{"duplicates collapse", []string{"app-feed-publish-a", "app-feed-publish-a"}, []string{"app.a"}, nil},
		{"the same stem in both directions stays in both", []string{"app-feed-publish-a", "app-feed-subscribe-a"}, []string{"app.a"}, []string{"app.a"}},
		{"unknown roles are ignored", []string{"admin", "sensor-feed-publish-x", "nats-tenant-observer", "role:reader"}, nil, nil},
		{"a prefix alone is nothing", []string{"app-feed-publish-", "app-feed-subscribe-"}, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pub, sub := SubjectsForRoles(c.roles)
			if !reflect.DeepEqual(pub, c.pub) || !reflect.DeepEqual(sub, c.sub) {
				t.Fatalf("got publish %v subscribe %v, want %v and %v", pub, sub, c.pub, c.sub)
			}
		})
	}
}

// A role can name one exact subject and nothing wider: no stem may produce a wildcard, a dot or an empty token.
func TestStemsCannotWidenTheGrant(t *testing.T) {
	for _, stem := range []string{
		">", "*", "a.b", "a.>", "a.*", "a--b", "-a", "a-", "A", "a_b", "a b", "a\tb", "ünïcode", "a>", "wild*", "",
	} {
		for _, prefix := range []string{AppPublishRolePrefix, AppSubscribeRolePrefix} {
			pub, sub := SubjectsForRoles([]string{prefix + stem})
			if len(pub)+len(sub) != 0 {
				t.Errorf("role %q granted %v %v", prefix+stem, pub, sub)
			}
		}
	}
}

func TestSubjectsHaveOnlyTheNamespaceAndLiteralTokens(t *testing.T) {
	pub, _ := SubjectsForRoles([]string{"app-feed-publish-a1-b2-c3"})
	if len(pub) != 1 || pub[0] != "app.a1.b2.c3" {
		t.Fatalf("got %v", pub)
	}
}

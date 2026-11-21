// SPDX-License-Identifier: Apache-2.0

package main

import "strings"

// The role-name mapper: the role names a ticket carries decide which subjects the minted user may
// publish to and subscribe to. The role name carries the whole decision. Its prefix says the
// direction and the rest says the subject, so no table or lookup is needed:
//
//	app-feed-publish-orders          -> publish   app.orders
//	app-feed-subscribe-alarms-high   -> subscribe app.alarms.high
//
// This is a generic re-implementation of the application-feed family of such a naming convention. It
// deliberately has no other families, no realm-role table and no wildcard grants: a role can never
// grant more than one exact subject, whatever its name.
const (
	AppSubjectNamespace    = "app"
	AppPublishRolePrefix   = "app-feed-publish-"
	AppSubscribeRolePrefix = "app-feed-subscribe-"
)

// SubjectsForRoles returns the subjects the roles entitle, deduplicated, in the order the roles
// first appear. Roles that are not recognized, or whose stem is not a dash-separated list of
// lowercase alphanumeric tokens, are ignored: the mapper fails closed, and a direction with no
// subjects is denied outright by the caller (see mintOperatorUser). Role names are matched as they
// are, which for NATS tags means lowercase: the jwt library lowercases tags when it adds them.
func SubjectsForRoles(roles []string) (publish, subscribe []string) {
	seenPub, seenSub := map[string]bool{}, map[string]bool{}
	for _, role := range roles {
		switch {
		case strings.HasPrefix(role, AppPublishRolePrefix):
			if s, ok := appSubject(strings.TrimPrefix(role, AppPublishRolePrefix)); ok && !seenPub[s] {
				seenPub[s] = true
				publish = append(publish, s)
			}
		case strings.HasPrefix(role, AppSubscribeRolePrefix):
			if s, ok := appSubject(strings.TrimPrefix(role, AppSubscribeRolePrefix)); ok && !seenSub[s] {
				seenSub[s] = true
				subscribe = append(subscribe, s)
			}
		}
	}
	return publish, subscribe
}

// appSubject turns a stem such as "alarms-high" into "app.alarms.high". It refuses an empty stem,
// an empty token ("a--b", a leading or trailing dash) and any character outside [a-z0-9] in a token,
// so a stem can never contain a dot or a wildcard.
func appSubject(stem string) (string, bool) {
	if stem == "" {
		return "", false
	}
	tokens := strings.Split(stem, "-")
	for _, tok := range tokens {
		if tok == "" {
			return "", false
		}
		for _, r := range tok {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
				return "", false
			}
		}
	}
	return AppSubjectNamespace + "." + strings.Join(tokens, "."), true
}

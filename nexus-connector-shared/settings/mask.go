package settings

import (
	"net/url"
	"strings"
)

// MaskURLPassword is a Variable's Mask for a URL that may hold a password, such as a proxy's
// http://user:password@proxy.example:8080: masked is the value with the password replaced by
// ****, and secrets are the password as written and, if different, unescaped. A URL without a
// password is shown as is. It finds the password as Go's net/url does, in the part before the
// last @ ahead of the first /, ? or #, after the first colon, with or without a scheme, as Go
// reads a proxy given as user:password@host:port too.
func MaskURLPassword(value string) (masked string, secrets []string) {
	start := 0
	if i := strings.Index(value, "://"); i >= 0 {
		start = i + len("://")
	}
	authority := value[start:]
	if i := strings.IndexAny(authority, "/?#"); i >= 0 {
		authority = authority[:i]
	}
	at := strings.LastIndex(authority, "@")
	if at < 0 {
		return value, nil
	}
	colon := strings.Index(authority[:at], ":")
	if colon < 0 || colon == at-1 {
		return value, nil // a user without a password, or an empty one
	}
	password := authority[colon+1 : at]
	secrets = []string{password}
	if unescaped, err := url.PathUnescape(password); err == nil && unescaped != password {
		secrets = append(secrets, unescaped)
	}
	return value[:start+colon+1] + "****" + value[start+at:], secrets
}

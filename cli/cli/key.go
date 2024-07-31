package cli

import "strings"

// DoesKeyMatch checks if a key matches a given pattern.
// Returns true if the key matches the pattern, false otherwise.
func DoesKeyMatch(filterPattern, key string) bool {
	patternTokens := strings.Split(filterPattern, ".")
	keyTokens := strings.Split(key, ".")

	if patternTokens[len(patternTokens)-1] == ">" {
		return matchWithTrailingWildcard(patternTokens[:len(patternTokens)-1], keyTokens)
	}

	if len(patternTokens) != len(keyTokens) {
		return false
	}

	return matchTokens(patternTokens, keyTokens)
}

func matchWithTrailingWildcard(patternTokens, keyTokens []string) bool {
	if len(keyTokens) <= len(patternTokens) {
		return false
	}
	return matchTokens(patternTokens, keyTokens[:len(patternTokens)])
}

func matchTokens(patternTokens, keyTokens []string) bool {
	for i := range patternTokens {
		if patternTokens[i] != "*" && patternTokens[i] != keyTokens[i] {
			return false
		}
	}
	return true
}

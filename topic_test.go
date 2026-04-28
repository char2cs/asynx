package asynx_test

import (
	"regexp"
	"testing"

	"github.com/char2cs/asynx"
)

func TestTopic_Literal_ExactMatch(t *testing.T) {
	pattern := asynx.Topic("arrow.add.github.com/char2cs/quiver.experiments")
	re := regexp.MustCompile(pattern)
	if !re.MatchString("arrow.add.github.com/char2cs/quiver.experiments") {
		t.Fatal("expected exact match")
	}
}

func TestTopic_Literal_RejectsDifferentNamespace(t *testing.T) {
	pattern := asynx.Topic("arrow.add.github.com/char2cs/quiver.experiments")
	re := regexp.MustCompile(pattern)
	if re.MatchString("arrow.add.github.com/char2cs/other") {
		t.Fatal("should not match different namespace")
	}
}

func TestTopic_Literal_DotsInNamespaceAreEscaped(t *testing.T) {
	pattern := asynx.Topic("arrow.add.github.com/char2cs/quiver.experiments")
	re := regexp.MustCompile(pattern)
	if re.MatchString("arrow.add.github.com/char2cs/quiverXexperiments") {
		t.Fatal("dot in namespace should be literal, not wildcard")
	}
}

func TestTopic_TrailingWildcard_MatchesAnyNamespace(t *testing.T) {
	pattern := asynx.Topic("arrow.add.*")
	re := regexp.MustCompile(pattern)
	if !re.MatchString("arrow.add.github.com/char2cs/quiver.experiments") {
		t.Fatal("trailing * should match any namespace")
	}
	if !re.MatchString("arrow.add.anything.with.dots") {
		t.Fatal("trailing * should match namespace with dots")
	}
}

func TestTopic_TrailingWildcard_RejectsDifferentAction(t *testing.T) {
	pattern := asynx.Topic("arrow.add.*")
	re := regexp.MustCompile(pattern)
	if re.MatchString("arrow.remove.github.com/char2cs/quiver.experiments") {
		t.Fatal("should not match different action")
	}
}

func TestTopic_MiddleWildcard_MatchesAnyAction(t *testing.T) {
	pattern := asynx.Topic("arrow.*.github.com/char2cs/quiver.experiments")
	re := regexp.MustCompile(pattern)
	if !re.MatchString("arrow.add.github.com/char2cs/quiver.experiments") {
		t.Fatal("middle * should match any action")
	}
	if !re.MatchString("arrow.remove.github.com/char2cs/quiver.experiments") {
		t.Fatal("middle * should match any action")
	}
}

func TestTopic_MiddleWildcard_RejectsExtraDots(t *testing.T) {
	pattern := asynx.Topic("arrow.*.github.com/char2cs/quiver.experiments")
	re := regexp.MustCompile(pattern)
	if re.MatchString("arrow.add.extra.github.com/char2cs/quiver.experiments") {
		t.Fatal("middle * should not match across dots")
	}
}

func TestTopic_AllOutputsAreValidRegex(t *testing.T) {
	patterns := []string{
		"arrow.add.github.com/char2cs/quiver.experiments",
		"arrow.add.*",
		"arrow.*",
		"arrow.*.github.com/char2cs/quiver.experiments",
		"*.*.*",
		"*",
	}
	for _, p := range patterns {
		result := asynx.Topic(p)
		if _, err := regexp.Compile(result); err != nil {
			t.Errorf("Topic(%q) = %q is not valid regex: %v", p, result, err)
		}
	}
}

func TestTopic_TwoSegments_TrailingWildcard(t *testing.T) {
	pattern := asynx.Topic("arrow.*")
	re := regexp.MustCompile(pattern)
	if !re.MatchString("arrow.add") {
		t.Fatal("should match arrow.add")
	}
	if !re.MatchString("arrow.remove") {
		t.Fatal("should match arrow.remove")
	}
	if !re.MatchString("arrow.add.something") {
		t.Fatal("two-segment trailing * should be greedy")
	}
}

func TestTopic_SingleSegment(t *testing.T) {
	pattern := asynx.Topic("arrow")
	re := regexp.MustCompile(pattern)
	if !re.MatchString("arrow") {
		t.Fatal("should match exact")
	}
	if re.MatchString("arrow.add") {
		t.Fatal("single segment should not match with extra parts")
	}
}

func TestTopic_AtSignInNamespace(t *testing.T) {
	pattern := asynx.Topic("arrow.add.github.com/org/dep@v1.0")
	re := regexp.MustCompile(pattern)
	if !re.MatchString("arrow.add.github.com/org/dep@v1.0") {
		t.Fatal("should match namespace with @ sign")
	}
}

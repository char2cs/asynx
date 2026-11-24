package channel

import "testing"

func TestCompiledPattern_MatchesAnywhereInTheName(
	t *testing.T,
) {
	compiled := compilePattern("Placed$")
	if !compiled.matches("OrderPlaced") {
		t.Error("Placed$ must match OrderPlaced")
	}

	if compiled.matches("OrderCancelled") {
		t.Error("Placed$ must not match OrderCancelled")
	}
}

func TestCompiledPattern_InvalidExpressionNeverMatchesAndKeepsItsError(
	t *testing.T,
) {
	compiled := compilePattern("^Order[")
	if compiled.err == nil {
		t.Fatal("an invalid expression must keep its compile error")
	}

	if compiled.matches("^Order[") {
		t.Error("an invalid expression must never match")
	}
}

func TestCompiledPattern_ZeroValueNeverMatches(
	t *testing.T,
) {
	var compiled compiledPattern

	if compiled.matches("anything") {
		t.Error("the zero value must never match")
	}
}

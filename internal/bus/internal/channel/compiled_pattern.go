package channel

import "regexp"

type compiledPattern struct {
	expr *regexp.Regexp
	err  error
}

func compilePattern(
	pattern string,
) compiledPattern {
	expr, err := regexp.Compile(pattern)

	return compiledPattern{
		expr: expr,
		err:  err,
	}
}

func (c compiledPattern) matches(
	eventName string,
) bool {
	return c.err == nil &&
		c.expr != nil &&
		c.expr.MatchString(eventName)
}

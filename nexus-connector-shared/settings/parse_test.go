package settings

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParse_Defaults(t *testing.T) {
	s, err := Text("")
	require.NoError(t, err)
	require.Empty(t, s)
	s, _ = TextOr("d")("")
	require.Equal(t, "d", s)
	n, _ := PositiveInt("N", 7)("")
	require.Equal(t, 7, n)
	b, _ := Bool("B")("")
	require.False(t, b)
	d, _ := Duration("D", time.Second)("")
	require.Equal(t, time.Second, d)
	m, _ := Minutes("M", 10*time.Minute)("")
	require.Equal(t, 10*time.Minute, m)
}

func TestParse_Values(t *testing.T) {
	s, _ := TextOr("d")("x")
	require.Equal(t, "x", s)
	s, err := RequiredText("R", "the thing")("x")
	require.NoError(t, err)
	require.Equal(t, "x", s)
	n, _ := PositiveInt("N", 7)("3")
	require.Equal(t, 3, n)
	b, _ := Bool("B")("true")
	require.True(t, b)
	d, _ := Duration("D", time.Second)("0s")
	require.Equal(t, time.Duration(0), d)
	m, _ := Minutes("M", 10*time.Minute)("2")
	require.Equal(t, 2*time.Minute, m)
}

// Each error names the variable and the bad value.
func TestParse_Errors(t *testing.T) {
	_, err := RequiredText("R", "the thing")("")
	require.EqualError(t, err, "set R: the thing")
	for _, bad := range []string{"0", "-1", "x"} {
		_, err := PositiveInt("N", 1)(bad)
		require.EqualError(t, err, `N must be a whole number, at least 1, not "`+bad+`"`)
		_, err = Minutes("M", time.Minute)(bad)
		require.EqualError(t, err, `M must be a whole number of minutes, at least 1, not "`+bad+`"`)
	}
	_, err = Bool("B")("maybe")
	require.EqualError(t, err, `B must be true or false, not "maybe"`)
	_, err = Duration("D", 0)("-1s")
	require.EqualError(t, err, `D must be a duration such as 500ms or 2s, not "-1s"`)
}

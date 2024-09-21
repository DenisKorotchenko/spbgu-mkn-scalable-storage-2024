package main

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)
	assert.Equal(t, slices.Contains(b[:], zero), true, "Should be at least one 'zero' element")
	assert.Equal(t, slices.Contains(b[:], one), true, "Should be at least one 'zero' element")
}

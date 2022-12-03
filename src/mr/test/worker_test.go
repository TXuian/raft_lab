package mr

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"6.824/mr"
)

func TestMapWorker(t *testing.T) {

	t.Run("test map input", func(t *testing.T) {
		t.Helper()

		buffer := &bytes.Buffer{}
		fmt.Fprintf(buffer, "this is is a a a input input input test test test test")
		got := mr.MapInput(Map, buffer, "mocking_input")

		want := []mr.KeyValue{
			{Key: "this", Value: "1"},
			{Key: "is", Value: "1"},
			{Key: "is", Value: "1"},
			{Key: "a", Value: "1"},
			{Key: "a", Value: "1"},
			{Key: "a", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, but wanted %v", got, want)
		}

	})

	t.Run("test map output", func(t *testing.T) {
		got := &bytes.Buffer{}

		intermediate_kv := []mr.KeyValue{
			{Key: "a", Value: "1"},
			{Key: "a", Value: "1"},
			{Key: "a", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "is", Value: "1"},
			{Key: "is", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "this", Value: "1"},
		}

		mr.MapOutput(got, &intermediate_kv)

		want := &bytes.Buffer{}
		fmt.Fprintf(want, "a 1\na 1\na 1\ninput 1\ninput 1\ninput 1\nis 1\nis 1\ntest 1\ntest 1\ntest 1\ntest 1\nthis 1\n")

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, but wanted %v", got, want)
		}

	})
}

func TestReduceWorker(t *testing.T) {

	t.Run("test reduce input", func(t *testing.T) {
		mock_ifile := &bytes.Buffer{}
		fmt.Fprintf(mock_ifile, "a 1\na 1\na 1\ninput 1\ninput 1\ninput 1\nis 1\nis 1\ntest 1\ntest 1\ntest 1\ntest 1\nthis 1\n")

		var got []mr.KeyValue
		mr.ReduceInput(mock_ifile, &got)

		want := []mr.KeyValue{
			{Key: "a", Value: "1"},
			{Key: "a", Value: "1"},
			{Key: "a", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "is", Value: "1"},
			{Key: "is", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "this", Value: "1"},
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, but wanted %v", got, want)
		}

	})

	t.Run("test reduce output", func(t *testing.T) {
		got := &bytes.Buffer{}

		intermediate_kv := []mr.KeyValue{
			{Key: "a", Value: "1"},
			{Key: "a", Value: "1"},
			{Key: "a", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "input", Value: "1"},
			{Key: "is", Value: "1"},
			{Key: "is", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "test", Value: "1"},
			{Key: "this", Value: "1"},
			{Key: "this", Value: "1"},
			{Key: "this", Value: "1"},
			{Key: "this", Value: "1"},
			{Key: "this", Value: "1"},
			{Key: "this", Value: "1"},
		}

		mr.ReduceOutput(Reduce, got, &intermediate_kv)

		want := &bytes.Buffer{}
		fmt.Fprintf(want, "a 3\ninput 3\nis 2\ntest 4\nthis 6\n")

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, but wanted %v", got, want)
		}

	})

}

func Map(filename string, contents string) []mr.KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}

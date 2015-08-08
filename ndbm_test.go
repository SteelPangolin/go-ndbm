package ndbm

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// items is a sortable list of database entries.
type items []Item

func (items items) Len() int {
	return len(items)
}

func (items items) Swap(i, j int) {
	items[i], items[j] = items[j], items[i]
}

func (items items) Less(i, j int) bool {
	return bytes.Compare(items[i].Key, items[j].Key) == -1
}

func TestNDBM(t *testing.T) {
	// Create a temp dir for test files.
	tempdir, err := ioutil.TempDir("", "TestNDBM")
	if err != nil {
		t.Fatalf("Couldn't create tempdir for test DB: %v", err)
	}
	defer os.RemoveAll(tempdir)

	// Create a new DB in that temp dir.
	ndbm, err := OpenWithDefaults(filepath.Join(tempdir, "test"))
	if err != nil {
		t.Fatalf("Couldn't open DB: %v", err)
	}
	defer ndbm.Close()

	// Check the empty database.
	if ndbm.Len() != 0 {
		t.Error("Empty DB should have no keys")
	}

	// Insert some data.
	{
		items := items{
			Item{[]byte("a"), []byte("alphabet")},
			Item{[]byte("b"), []byte("battlement")},
			Item{[]byte("c"), []byte("carnival")},
			Item{[]byte("d"), []byte("dinosaur")},
		}
		err := ndbm.Update(items)
		if err != nil {
			t.Errorf("Error on update: %v", err)
		}
		if ndbm.Len() != 4 {
			t.Errorf("DB should have 4 keys, but actually has %d", ndbm.Len())
		}
	}

	// Try to fetch a key that doesn't exist, which should fail.
	{
		value, err := ndbm.Fetch([]byte("x"))
		if value != nil || err == nil {
			t.Errorf("Expected error on fetch")
		} else {
			_, expected := err.(KeyNotFound)
			if !expected {
				t.Errorf("Error on fetch: %v", err)
			}
		}
	}

	// Try to insert a key that already exists, which should fail.
	{
		err := ndbm.Insert([]byte("c"), []byte("contentment"))
		if err == nil {
			t.Errorf("Expected error on insert")
		} else {
			_, expected := err.(KeyAlreadyExists)
			if !expected {
				t.Errorf("Error on insert: %v", err)
			}
		}
		if ndbm.Len() != 4 {
			t.Errorf("DB should still have 4 keys, but actually has %d", ndbm.Len())
		}
	}

	// Replace a key that already exists.
	{
		err := ndbm.Replace([]byte("c"), []byte("contentment"))
		if err != nil {
			t.Errorf("Error on replace: %v", err)
		}
		if ndbm.Len() != 4 {
			t.Errorf("DB should still have 4 keys, but actually has %d", ndbm.Len())
		}
	}

	// Delete a key.
	{
		err := ndbm.Delete([]byte("b"))
		if err != nil {
			t.Errorf("Error on delete: %v", err)
		}
		if ndbm.Len() != 3 {
			t.Errorf("DB should have 3 keys, but actually has %d", ndbm.Len())
		}
	}

	// Delete a key that has already been deleted, which should fail.
	{
		err := ndbm.Delete([]byte("b"))
		if err == nil {
			t.Errorf("Expected error on delete")
		} else {
			_, expected := err.(KeyNotFound)
			if !expected {
				t.Errorf("Error on delete: %v", err)
			}
		}
		if ndbm.Len() != 3 {
			t.Errorf("DB should have 3 keys, but actually has %d", ndbm.Len())
		}
	}

	// Delete a key that has never existed, which should fail.
	{
		err := ndbm.Delete([]byte("x"))
		if err == nil {
			t.Errorf("Expected error on delete")
		} else {
			_, expected := err.(KeyNotFound)
			if !expected {
				t.Errorf("Error on delete: %v", err)
			}
		}
	}

	// Get all contents, and see if it's what we expected.
	{
		expected := items{
			Item{[]byte("a"), []byte("alphabet")},
			Item{[]byte("c"), []byte("contentment")},
			Item{[]byte("d"), []byte("dinosaur")},
		}
		actual := items(ndbm.Items())
		if len(expected) != len(actual) {
			t.Fatalf(
				"Expected and actual DB contents have different lengths: %d vs. %d",
				len(expected), len(actual))
		}
		sort.Sort(actual)
		for i, expectedItem := range expected {
			actualItem := actual[i]
			if bytes.Compare(expectedItem.Key, actualItem.Key) != 0 {
				t.Errorf("Expected and actual items %d have different keys: %s vs. %s",
					i, expectedItem.Key, actualItem.Key)
			}
			if bytes.Compare(expectedItem.Value, actualItem.Value) != 0 {
				t.Errorf("Expected and actual items %d have different values: %s vs. %s",
					i, expectedItem.Value, actualItem.Value)
			}
		}
	}
}

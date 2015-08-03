package ndbm

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestNDBM(t *testing.T) {
	// create a temp dir for test files
	tempdir, err := ioutil.TempDir("", "TestNDBM")
	if err != nil {
		t.Fatalf("Couldn't create tempdir for test DB: %v", err)
	}
	defer os.RemoveAll(tempdir)

	// create a new DB in that temp dir
	ndbm, err := OpenWithDefaults(filepath.Join(tempdir, "test"))
	if err != nil {
		t.Fatalf("Couldn't open DB: %v", err)
	}
	defer ndbm.Close()

	// check the empty database
	if ndbm.Len() != 0 {
		t.Error("Empty DB should have no keys")
	}

	// insert some data
	{
		items := Items{
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

	// try to fetch a key that doesn't exist, which should fail
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

	// try to insert a key that already exists, which should fail
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

	// replace a key that already exists
	{
		err := ndbm.Replace([]byte("c"), []byte("contentment"))
		if err != nil {
			t.Errorf("Error on replace: %v", err)
		}
		if ndbm.Len() != 4 {
			t.Errorf("DB should still have 4 keys, but actually has %d", ndbm.Len())
		}
	}

	// delete a key
	{
		err := ndbm.Delete([]byte("b"))
		if err != nil {
			t.Errorf("Error on delete: %v", err)
		}
		if ndbm.Len() != 3 {
			t.Errorf("DB should have 3 keys, but actually has %d", ndbm.Len())
		}
	}

	// delete a key that has already been deleted, which should fail
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

	// delete a key that has never existed, which should fail
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

	// get all contents, see if it's what we expected
	{
		expected := Items{
			Item{[]byte("a"), []byte("alphabet")},
			Item{[]byte("c"), []byte("contentment")},
			Item{[]byte("d"), []byte("dinosaur")},
		}
		actual := ndbm.Items()
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
					expectedItem.Key, actualItem.Key)
			}
			if bytes.Compare(expectedItem.Value, actualItem.Value) != 0 {
				t.Errorf("Expected and actual items %d have different values: %s vs. %s",
					expectedItem.Value, actualItem.Value)
			}
		}
	}
}

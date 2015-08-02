package dbm

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestDBM(t *testing.T) {
	// create a temp dir for test files
	tempdir, err := ioutil.TempDir("", "TestDBM")
	if err != nil {
		t.Fatalf("Couldn't create tempdir for test DB: %v", err)
	}
	defer os.RemoveAll(tempdir)

	// create a new DB in that temp dir
	dbm, err := Open(filepath.Join(tempdir, "test"))
	if err != nil {
		t.Fatalf("Couldn't open DB: %v", err)
	}
	defer dbm.Close()

	// check the empty database
	if dbm.Len() != 0 {
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
		err := dbm.Update(items)
		if err != nil {
			t.Errorf("Error on update: %v", err)
		}
		if dbm.Len() != 4 {
			t.Errorf("DB should have 4 keys, but actually has %d", dbm.Len())
		}
	}

	// try to insert a key that already exists, which should fail
	{
		err := dbm.Insert([]byte("c"), []byte("contentment"))
		if err != nil {
			_, expected := err.(KeyAlreadyExists)
			if !expected {
				t.Errorf("Error on insert: %v", err)
			}
		}
		if dbm.Len() != 4 {
			t.Errorf("DB should still have 4 keys, but actually has %d", dbm.Len())
		}
	}

	// replace a key that already exists
	{
		err := dbm.Replace([]byte("c"), []byte("contentment"))
		if err != nil {
			t.Errorf("Error on replace: %v", err)
		}
		if dbm.Len() != 4 {
			t.Errorf("DB should still have 4 keys, but actually has %d", dbm.Len())
		}
	}

	// delete a key
	{
		err := dbm.Delete([]byte("b"))
		if err != nil {
			t.Errorf("Error on delete: %v", err)
		}
		if dbm.Len() != 3 {
			t.Errorf("DB should have 3 keys, but actually has %d", dbm.Len())
		}
	}

	// delete a key that has already been deleted
	{
		err := dbm.Delete([]byte("b"))
		if err != nil {
			_, expected := err.(KeyNotFound)
			if !expected {
				t.Errorf("Error on delete: %v", err)
			}
		}
		if dbm.Len() != 3 {
			t.Errorf("DB should have 3 keys, but actually has %d", dbm.Len())
		}
	}

	// delete a key that has never existed
	{
		err := dbm.Delete([]byte("x"))
		if err != nil {
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
		actual := dbm.Items()
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

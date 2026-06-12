package abstract_sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type revisionTestSqlGen struct{}

func (revisionTestSqlGen) GetSqlInsert(tableName string) string {
	return fmt.Sprintf(`INSERT INTO "%s" (dirhash,name,directory,meta) VALUES(?,?,?,?)`, tableName)
}

func (revisionTestSqlGen) GetSqlUpdate(tableName string) string {
	return fmt.Sprintf(`UPDATE "%s" SET meta=? WHERE dirhash=? AND name=? AND directory=?`, tableName)
}

func (revisionTestSqlGen) GetSqlFind(tableName string) string {
	return fmt.Sprintf(`SELECT meta FROM "%s" WHERE dirhash=? AND name=? AND directory=?`, tableName)
}

func (revisionTestSqlGen) GetSqlDelete(tableName string) string {
	return fmt.Sprintf(`DELETE FROM "%s" WHERE dirhash=? AND name=? AND directory=?`, tableName)
}

func (revisionTestSqlGen) GetSqlDeleteFolderChildren(tableName string) string {
	return fmt.Sprintf(`DELETE FROM "%s" WHERE dirhash=? AND directory=?`, tableName)
}

func (revisionTestSqlGen) GetSqlListExclusive(tableName string) string {
	return fmt.Sprintf(`SELECT NAME, meta FROM "%s" WHERE dirhash=? AND name>? AND directory=? AND name like ? ORDER BY NAME ASC LIMIT ?`, tableName)
}

func (revisionTestSqlGen) GetSqlListInclusive(tableName string) string {
	return fmt.Sprintf(`SELECT NAME, meta FROM "%s" WHERE dirhash=? AND name>=? AND directory=? AND name like ? ORDER BY NAME ASC LIMIT ?`, tableName)
}

func (revisionTestSqlGen) GetSqlCreateTable(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
		dirhash INTEGER,
		name TEXT,
		directory TEXT,
		meta BLOB,
		PRIMARY KEY (dirhash, name)
	)`, tableName)
}

func (revisionTestSqlGen) GetSqlDropTable(tableName string) string {
	return fmt.Sprintf(`DROP TABLE "%s"`, tableName)
}

func (revisionTestSqlGen) GetSqlInsertWithRevision(tableName string) string {
	return fmt.Sprintf(`INSERT INTO "%s" (dirhash,name,directory,meta,entry_revision) VALUES(?,?,?,?,1) RETURNING entry_revision`, tableName)
}

func (revisionTestSqlGen) GetSqlUpdateWithRevision(tableName string) string {
	return fmt.Sprintf(`UPDATE "%s" SET meta=?, entry_revision=entry_revision+1 WHERE dirhash=? AND name=? AND directory=? AND entry_revision=? RETURNING entry_revision`, tableName)
}

func (revisionTestSqlGen) GetSqlFindWithRevision(tableName string) string {
	return fmt.Sprintf(`SELECT meta, entry_revision FROM "%s" WHERE dirhash=? AND name=? AND directory=?`, tableName)
}

func (revisionTestSqlGen) GetSqlListExclusiveWithRevision(tableName string) string {
	return fmt.Sprintf(`SELECT NAME, meta, entry_revision FROM "%s" WHERE dirhash=? AND name>? AND directory=? AND name like ? ORDER BY NAME ASC LIMIT ?`, tableName)
}

func (revisionTestSqlGen) GetSqlListInclusiveWithRevision(tableName string) string {
	return fmt.Sprintf(`SELECT NAME, meta, entry_revision FROM "%s" WHERE dirhash=? AND name>=? AND directory=? AND name like ? ORDER BY NAME ASC LIMIT ?`, tableName)
}

func (revisionTestSqlGen) GetSqlEnsureEntryRevisionColumn(tableName string) string {
	return fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN entry_revision INTEGER NOT NULL DEFAULT 0`, tableName)
}

func TestStrictEntryRevisionRejectsStaleUpdate(t *testing.T) {
	store := newRevisionTestStore(t)
	ctx := context.Background()

	entry := &filer.Entry{
		FullPath: "/dir/file.txt",
		Attr: filer.Attr{
			Mode: 0o660,
			Mime: "text/plain",
		},
	}
	require.NoError(t, store.InsertEntry(ctx, entry))
	require.Equal(t, int64(1), entry.Revision)

	found, err := store.FindEntry(ctx, entry.FullPath)
	require.NoError(t, err)
	require.Equal(t, int64(1), found.Revision)

	stale := found.ShallowClone()
	stale.Revision = 0
	stale.Mime = "application/json"
	err = store.UpdateEntry(ctx, stale)
	require.Error(t, err)
	require.True(t, errors.Is(err, filer.ErrMetadataRevisionMismatch), "got %v", err)

	current, err := store.FindEntry(ctx, entry.FullPath)
	require.NoError(t, err)
	require.Equal(t, int64(1), current.Revision)
	require.Equal(t, "text/plain", current.Mime)

	current.Mime = "application/json"
	require.NoError(t, store.UpdateEntry(ctx, current))
	require.Equal(t, int64(2), current.Revision)

	updated, err := store.FindEntry(ctx, entry.FullPath)
	require.NoError(t, err)
	require.Equal(t, int64(2), updated.Revision)
	require.Equal(t, "application/json", updated.Mime)
}

func TestStrictEntryRevisionPreservedInDirectoryListings(t *testing.T) {
	store := newRevisionTestStore(t)
	ctx := context.Background()

	entry := &filer.Entry{
		FullPath: "/dir/file.txt",
		Attr:     filer.Attr{Mode: 0o660},
	}
	require.NoError(t, store.InsertEntry(ctx, entry))

	var listed []*filer.Entry
	_, err := store.ListDirectoryEntries(ctx, util.FullPath("/dir"), "", false, 10, func(entry *filer.Entry) (bool, error) {
		listed = append(listed, entry)
		return true, nil
	})
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, int64(1), listed[0].Revision)
}

func newRevisionTestStore(t *testing.T) *AbstractSqlStore {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	store := &AbstractSqlStore{
		DB:                  db,
		SqlGenerator:        revisionTestSqlGen{},
		SupportBucketTable:  true,
		StrictEntryRevision: true,
	}
	require.NoError(t, store.CreateTable(context.Background(), DEFAULT_TABLE))
	return store
}

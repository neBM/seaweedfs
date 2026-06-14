package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCreateEntryTouchesParentBeforeChildEvent(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/maildir"] = newDirectoryEntry("/maildir", 10)
	store.entries["/maildir/.Trash"] = newDirectoryEntry("/maildir/.Trash", 11)
	parentBefore, err := store.FindEntry(context.Background(), util.FullPath("/maildir/.Trash"))
	if err != nil {
		t.Fatalf("find parent before create: %v", err)
	}

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(store), option: &FilerOption{}}
	resp, err := server.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: "/maildir/.Trash",
		Entry: &filer_pb.Entry{
			Name: "tmp-file",
			Attributes: &filer_pb.FuseAttributes{
				FileMode: 0o644,
				Uid:      1000,
				Gid:      1000,
				Crtime:   time.Now().Unix(),
				Mtime:    time.Now().Unix(),
				Ctime:    time.Now().Unix(),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateEntry: %v", err)
	}
	if resp.GetMetadataEvent() == nil {
		t.Fatal("expected child metadata event in CreateEntry response")
	}
	if got := resp.GetMetadataEvent().GetDirectory(); got != "/maildir/.Trash" {
		t.Fatalf("response event directory = %q, want /maildir/.Trash", got)
	}
	if got := resp.GetMetadataEvent().GetEventNotification().GetNewEntry().GetName(); got != "tmp-file" {
		t.Fatalf("response child entry = %q, want tmp-file", got)
	}

	events := queue.snapshot()
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2", len(events))
	}

	parentEvent := events[0]
	if parentEvent.key != "/maildir/.Trash" {
		t.Fatalf("parent event key = %q, want /maildir/.Trash", parentEvent.key)
	}
	if parentEvent.notification.NewEntry == nil || parentEvent.notification.NewEntry.Name != ".Trash" {
		t.Fatalf("parent event new entry = %+v, want .Trash", parentEvent.notification.NewEntry)
	}

	childEvent := events[1]
	if childEvent.key != "/maildir/.Trash/tmp-file" {
		t.Fatalf("child event key = %q, want /maildir/.Trash/tmp-file", childEvent.key)
	}
	if childEvent.notification.NewEntry == nil || childEvent.notification.NewEntry.Name != "tmp-file" {
		t.Fatalf("child event new entry = %+v, want tmp-file", childEvent.notification.NewEntry)
	}

	parentAfter, err := store.FindEntry(context.Background(), util.FullPath("/maildir/.Trash"))
	if err != nil {
		t.Fatalf("find parent after create: %v", err)
	}
	if !parentAfter.Attr.Mtime.After(parentBefore.Attr.Mtime) {
		t.Fatalf("parent mtime = %v, want > %v", parentAfter.Attr.Mtime, parentBefore.Attr.Mtime)
	}
	if !parentAfter.Attr.Ctime.After(parentBefore.Attr.Ctime) {
		t.Fatalf("parent ctime = %v, want > %v", parentAfter.Attr.Ctime, parentBefore.Attr.Ctime)
	}
}

func TestDeleteEntryTouchesParentBeforeChildEvent(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/maildir"] = newDirectoryEntry("/maildir", 10)
	store.entries["/maildir/.Trash"] = newDirectoryEntry("/maildir/.Trash", 11)
	store.entries["/maildir/.Trash/tmp-file"] = newFileEntry("/maildir/.Trash/tmp-file", 12)
	parentBefore, err := store.FindEntry(context.Background(), util.FullPath("/maildir/.Trash"))
	if err != nil {
		t.Fatalf("find parent before delete: %v", err)
	}

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(store), option: &FilerOption{}}
	resp, err := server.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
		Directory:    "/maildir/.Trash",
		Name:         "tmp-file",
		IsDeleteData: true,
	})
	if err != nil {
		t.Fatalf("DeleteEntry: %v", err)
	}
	if resp.GetMetadataEvent() == nil {
		t.Fatal("expected child metadata event in DeleteEntry response")
	}
	if got := resp.GetMetadataEvent().GetDirectory(); got != "/maildir/.Trash" {
		t.Fatalf("response event directory = %q, want /maildir/.Trash", got)
	}
	if got := resp.GetMetadataEvent().GetEventNotification().GetOldEntry().GetName(); got != "tmp-file" {
		t.Fatalf("response child entry = %q, want tmp-file", got)
	}

	events := queue.snapshot()
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2", len(events))
	}

	parentEvent := events[0]
	if parentEvent.key != "/maildir/.Trash" {
		t.Fatalf("parent event key = %q, want /maildir/.Trash", parentEvent.key)
	}
	if parentEvent.notification.NewEntry == nil || parentEvent.notification.NewEntry.Name != ".Trash" {
		t.Fatalf("parent event new entry = %+v, want .Trash", parentEvent.notification.NewEntry)
	}

	childEvent := events[1]
	if childEvent.key != "/maildir/.Trash/tmp-file" {
		t.Fatalf("child event key = %q, want /maildir/.Trash/tmp-file", childEvent.key)
	}
	if childEvent.notification.OldEntry == nil || childEvent.notification.OldEntry.Name != "tmp-file" {
		t.Fatalf("child event old entry = %+v, want tmp-file", childEvent.notification.OldEntry)
	}

	parentAfter, err := store.FindEntry(context.Background(), util.FullPath("/maildir/.Trash"))
	if err != nil {
		t.Fatalf("find parent after delete: %v", err)
	}
	if !parentAfter.Attr.Mtime.After(parentBefore.Attr.Mtime) {
		t.Fatalf("parent mtime = %v, want > %v", parentAfter.Attr.Mtime, parentBefore.Attr.Mtime)
	}
	if !parentAfter.Attr.Ctime.After(parentBefore.Attr.Ctime) {
		t.Fatalf("parent ctime = %v, want > %v", parentAfter.Attr.Ctime, parentBefore.Attr.Ctime)
	}
}

func TestCreateEntryUpdateDoesNotTouchParent(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/maildir"] = newDirectoryEntry("/maildir", 10)
	store.entries["/maildir/.Trash"] = newDirectoryEntry("/maildir/.Trash", 11)
	store.entries["/maildir/.Trash/tmp-file"] = newFileEntry("/maildir/.Trash/tmp-file", 12)
	parentBefore, err := store.FindEntry(context.Background(), util.FullPath("/maildir/.Trash"))
	if err != nil {
		t.Fatalf("find parent before update: %v", err)
	}

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(store), option: &FilerOption{}}
	resp, err := server.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: "/maildir/.Trash",
		Entry: &filer_pb.Entry{
			Name: "tmp-file",
			Attributes: &filer_pb.FuseAttributes{
				FileMode: 0o600,
				Uid:      1000,
				Gid:      1000,
				Crtime:   time.Now().Unix(),
				Mtime:    time.Now().Unix(),
				Ctime:    time.Now().Unix(),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateEntry update: %v", err)
	}
	if resp.GetMetadataEvent() == nil {
		t.Fatal("expected child metadata event in CreateEntry update response")
	}
	if got := resp.GetMetadataEvent().GetEventNotification().GetNewEntry().GetName(); got != "tmp-file" {
		t.Fatalf("response child entry = %q, want tmp-file", got)
	}

	events := queue.snapshot()
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	if events[0].key != "/maildir/.Trash/tmp-file" {
		t.Fatalf("child event key = %q, want /maildir/.Trash/tmp-file", events[0].key)
	}

	parentAfter, err := store.FindEntry(context.Background(), util.FullPath("/maildir/.Trash"))
	if err != nil {
		t.Fatalf("find parent after update: %v", err)
	}
	if !parentAfter.Attr.Mtime.Equal(parentBefore.Attr.Mtime) {
		t.Fatalf("parent mtime = %v, want unchanged %v", parentAfter.Attr.Mtime, parentBefore.Attr.Mtime)
	}
	if !parentAfter.Attr.Ctime.Equal(parentBefore.Attr.Ctime) {
		t.Fatalf("parent ctime = %v, want unchanged %v", parentAfter.Attr.Ctime, parentBefore.Attr.Ctime)
	}
}

func TestAtomicRenameEntryTouchesParentsBeforeRenameEvent(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/maildir"] = newDirectoryEntry("/maildir", 9)
	store.entries["/maildir/tmp"] = newDirectoryEntry("/maildir/tmp", 10)
	store.entries["/maildir/cur"] = newDirectoryEntry("/maildir/cur", 11)
	store.entries["/maildir/tmp/tmp-file"] = newFileEntry("/maildir/tmp/tmp-file", 12)
	srcBefore, err := store.FindEntry(context.Background(), util.FullPath("/maildir/tmp"))
	if err != nil {
		t.Fatalf("find src before rename: %v", err)
	}
	dstBefore, err := store.FindEntry(context.Background(), util.FullPath("/maildir/cur"))
	if err != nil {
		t.Fatalf("find dst before rename: %v", err)
	}

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(store)}
	_, err = server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/maildir/tmp",
		OldName:      "tmp-file",
		NewDirectory: "/maildir/cur",
		NewName:      "cur-file",
	})
	if err != nil {
		t.Fatalf("AtomicRenameEntry: %v", err)
	}

	events := queue.snapshot()
	if len(events) != 3 {
		t.Fatalf("event count = %d, want 3", len(events))
	}

	if events[0].key != "/maildir/tmp" {
		t.Fatalf("first event key = %q, want /maildir/tmp", events[0].key)
	}
	if events[1].key != "/maildir/cur" {
		t.Fatalf("second event key = %q, want /maildir/cur", events[1].key)
	}
	if events[2].key != "/maildir/tmp/tmp-file" {
		t.Fatalf("rename event key = %q, want /maildir/tmp/tmp-file", events[2].key)
	}
	if events[2].notification.NewEntry == nil || events[2].notification.NewEntry.Name != "cur-file" {
		t.Fatalf("rename new entry = %+v, want cur-file", events[2].notification.NewEntry)
	}
	if events[2].notification.NewParentPath != "/maildir/cur" {
		t.Fatalf("rename new parent path = %q, want /maildir/cur", events[2].notification.NewParentPath)
	}

	srcAfter, err := store.FindEntry(context.Background(), util.FullPath("/maildir/tmp"))
	if err != nil {
		t.Fatalf("find src after rename: %v", err)
	}
	dstAfter, err := store.FindEntry(context.Background(), util.FullPath("/maildir/cur"))
	if err != nil {
		t.Fatalf("find dst after rename: %v", err)
	}
	if !srcAfter.Attr.Mtime.After(srcBefore.Attr.Mtime) {
		t.Fatalf("src parent mtime = %v, want > %v", srcAfter.Attr.Mtime, srcBefore.Attr.Mtime)
	}
	if !dstAfter.Attr.Mtime.After(dstBefore.Attr.Mtime) {
		t.Fatalf("dst parent mtime = %v, want > %v", dstAfter.Attr.Mtime, dstBefore.Attr.Mtime)
	}
}

func TestAtomicRenameEntryTouchesSameParentOnceBeforeRenameEvent(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/maildir"] = newDirectoryEntry("/maildir", 9)
	store.entries["/maildir/cur"] = newDirectoryEntry("/maildir/cur", 10)
	store.entries["/maildir/cur/tmp-file"] = newFileEntry("/maildir/cur/tmp-file", 11)
	parentBefore, err := store.FindEntry(context.Background(), util.FullPath("/maildir/cur"))
	if err != nil {
		t.Fatalf("find parent before rename: %v", err)
	}

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(store)}
	_, err = server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/maildir/cur",
		OldName:      "tmp-file",
		NewDirectory: "/maildir/cur",
		NewName:      "cur-file",
	})
	if err != nil {
		t.Fatalf("AtomicRenameEntry same parent: %v", err)
	}

	events := queue.snapshot()
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2", len(events))
	}

	if events[0].key != "/maildir/cur" {
		t.Fatalf("first event key = %q, want /maildir/cur", events[0].key)
	}
	if events[1].key != "/maildir/cur/tmp-file" {
		t.Fatalf("rename event key = %q, want /maildir/cur/tmp-file", events[1].key)
	}
	if events[1].notification.NewEntry == nil || events[1].notification.NewEntry.Name != "cur-file" {
		t.Fatalf("rename new entry = %+v, want cur-file", events[1].notification.NewEntry)
	}

	parentAfter, err := store.FindEntry(context.Background(), util.FullPath("/maildir/cur"))
	if err != nil {
		t.Fatalf("find parent after rename: %v", err)
	}
	if !parentAfter.Attr.Mtime.After(parentBefore.Attr.Mtime) {
		t.Fatalf("parent mtime = %v, want > %v", parentAfter.Attr.Mtime, parentBefore.Attr.Mtime)
	}
}

package weed_server

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

type ChunkReferenceGuard struct {
	filerAddresses []pb.ServerAddress
	grpcDialOption grpc.DialOption
	checkFn        func(context.Context, *filer_pb.CheckChunkReferencesRequest) (*filer_pb.CheckChunkReferencesResponse, error)
}

func NewChunkReferenceGuard(filerAddresses []pb.ServerAddress, grpcDialOption grpc.DialOption) *ChunkReferenceGuard {
	if len(filerAddresses) == 0 {
		return nil
	}
	return &ChunkReferenceGuard{
		filerAddresses: filerAddresses,
		grpcDialOption: grpcDialOption,
	}
}

func newChunkReferenceGuardForTest(checkFn func(context.Context, *filer_pb.CheckChunkReferencesRequest) (*filer_pb.CheckChunkReferencesResponse, error)) *ChunkReferenceGuard {
	return &ChunkReferenceGuard{checkFn: checkFn}
}

func (g *ChunkReferenceGuard) CheckFileIds(ctx context.Context, fileIds []string) error {
	protected, err := g.ProtectedFileIds(ctx, fileIds)
	if err != nil {
		return err
	}
	if len(protected) > 0 {
		var descriptions []string
		for _, description := range protected {
			descriptions = append(descriptions, description)
		}
		sort.Strings(descriptions)
		return fmt.Errorf("chunk reference guard blocked protected file ids: %s", strings.Join(descriptions, "; "))
	}
	return nil
}

func (g *ChunkReferenceGuard) ProtectedFileIds(ctx context.Context, fileIds []string) (map[string]string, error) {
	protected := make(map[string]string)
	if g == nil || len(fileIds) == 0 {
		return protected, nil
	}
	resp, err := g.check(ctx, &filer_pb.CheckChunkReferencesRequest{
		FileIds:      fileIds,
		IncludePaths: true,
	})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("chunk reference guard received empty filer response")
	}
	for fileId, status := range resp.ChunkStatus {
		if status == nil {
			continue
		}
		if !status.Referenced && !status.Leased {
			continue
		}
		reason := "referenced"
		if status.Leased {
			reason = "leased"
		}
		if len(status.Paths) > 0 {
			protected[fileId] = fmt.Sprintf("%s (%s: %s)", fileId, reason, strings.Join(status.Paths, ","))
		} else {
			protected[fileId] = fmt.Sprintf("%s (%s)", fileId, reason)
		}
	}
	return protected, nil
}

func (g *ChunkReferenceGuard) CheckCompactIndex(ctx context.Context, volumeId needle.VolumeId, compactIndexFileName string) error {
	if g == nil {
		return nil
	}
	presentKeys, err := storage.CollectLiveFileKeysFromIndex(compactIndexFileName)
	if err != nil {
		return fmt.Errorf("read compacted index %s: %w", compactIndexFileName, err)
	}
	resp, err := g.check(ctx, &filer_pb.CheckChunkReferencesRequest{
		VolumeId:        uint32(volumeId),
		PresentFileKeys: presentKeys,
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("chunk reference guard received empty filer response")
	}
	if len(resp.MissingFileIds) > 0 {
		return fmt.Errorf("chunk reference guard blocked vacuum commit for volume %d: compacted index is missing protected file ids %s", volumeId, strings.Join(resp.MissingFileIds, ","))
	}
	return nil
}

func (g *ChunkReferenceGuard) check(ctx context.Context, req *filer_pb.CheckChunkReferencesRequest) (*filer_pb.CheckChunkReferencesResponse, error) {
	if g.checkFn != nil {
		return g.checkFn(ctx, req)
	}
	var resp *filer_pb.CheckChunkReferencesResponse
	err := pb.WithOneOfGrpcFilerClients(false, g.filerAddresses, g.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		var checkErr error
		resp, checkErr = client.CheckChunkReferences(ctx, req)
		return checkErr
	})
	if err != nil {
		return nil, fmt.Errorf("chunk reference guard could not verify filer metadata: %w", err)
	}
	return resp, nil
}

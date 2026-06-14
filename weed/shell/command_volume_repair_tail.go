package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func init() {
	Commands = append(Commands, &commandVolumeRepairTail{})
}

type commandVolumeRepairTail struct {
}

func (c *commandVolumeRepairTail) Name() string {
	return "volume.repair.tail"
}

func (c *commandVolumeRepairTail) Help() string {
	return `repair tail-only regular volume corruption on a specific volume server.

	volume.repair.tail -node <volume server host:port> -volumeId <volume id>[,<volume id>...] [-apply] [-markWritable] [-backupDir <dir>]

	This is intended for read-only single-replica volumes where corruption is confined
	to a trailing data or index tail. In apply mode the volume is unmounted, backed up,
	truncated, remounted, scrubbed again, and optionally marked writable.
`
}

func (c *commandVolumeRepairTail) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeRepairTail) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	repairCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	nodeStr := repairCommand.String("node", "", "the volume server <host>:<port>")
	volumeIDsStr := repairCommand.String("volumeId", "", "comma-separated volume IDs to repair")
	applyChanges := repairCommand.Bool("apply", false, "apply the repair")
	markWritable := repairCommand.Bool("markWritable", false, "mark the repaired volume writable after a clean post-repair scrub")
	backupDir := repairCommand.String("backupDir", "", "directory on the volume server host for backups")
	if err = repairCommand.Parse(args); err != nil {
		return err
	}

	infoAboutSimulationMode(writer, *applyChanges, "-apply")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return err
	}
	if *nodeStr == "" {
		return fmt.Errorf("-node is required")
	}
	if *volumeIDsStr == "" {
		return fmt.Errorf("-volumeId is required")
	}

	var volumeIDs []uint32
	for _, token := range strings.Split(*volumeIDsStr, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		vid, parseErr := strconv.ParseUint(token, 10, 32)
		if parseErr != nil {
			return fmt.Errorf("invalid volume ID %q", token)
		}
		volumeIDs = append(volumeIDs, uint32(vid))
	}
	if len(volumeIDs) == 0 {
		return fmt.Errorf("-volumeId did not contain any volume IDs")
	}

	for _, vid := range volumeIDs {
		fmt.Fprintf(writer, "Repairing %s volume %d...\n", *nodeStr, vid)

		err = operation.WithVolumeServerClient(false, pb.ServerAddress(*nodeStr), commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			resp, repairErr := volumeServerClient.RepairVolumeTail(context.Background(), &volume_server_pb.RepairVolumeTailRequest{
				VolumeId:     vid,
				Apply:        *applyChanges,
				MarkWritable: *markWritable,
				BackupDir:    *backupDir,
			})
			if repairErr != nil {
				return repairErr
			}

			for _, detail := range resp.GetDetails() {
				fmt.Fprintf(writer, "  %s\n", detail)
			}
			for _, backupFile := range resp.GetBackupFiles() {
				fmt.Fprintf(writer, "  backup: %s\n", backupFile)
			}
			if resp.GetApplied() {
				fmt.Fprintf(writer, "  applied repair for volume %d\n", vid)
			}
			if resp.GetMarkedWritable() {
				fmt.Fprintf(writer, "  volume %d is writable again\n", vid)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("repair volume %d on %s: %w", vid, *nodeStr, err)
		}
	}

	return nil
}

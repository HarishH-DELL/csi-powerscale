package service

import (
	"context"
	"fmt"
	"github.com/dell/csi-isilon/common/utils"
	podmon "github.com/dell/dell-csi-extensions/podmon"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *service) ValidateVolumeHostConnectivity(ctx context.Context, req *podmon.ValidateVolumeHostConnectivityRequest) (*podmon.ValidateVolumeHostConnectivityResponse, error) {
	ctx, log, _ := GetRunIDLog(ctx)
	log.Infof("ValidateVolumeHostConnectivity called %+v", req)
	rep := &podmon.ValidateVolumeHostConnectivityResponse{
		Messages: make([]string, 0),
	}

	if (len(req.GetVolumeIds()) == 0 || len(req.GetVolumeIds()) == 0) && req.GetNodeId() == "" {
		// This is a nop call just testing the interface is present
		rep.Messages = append(rep.Messages, "ValidateVolumeHostConnectivity is implemented")
		return rep, nil
	}

	clusterName := s.defaultIsiClusterName
	//Get cluster config
	isiConfig, err := s.getIsilonConfig(ctx, &clusterName)
	if err != nil {
		log.Error("Failed to get Isilon config with error ", err.Error())
		return nil, err
	}

	//set cluster context
	ctx, log = setClusterContext(ctx, clusterName)
	log.Debugf("Cluster Name: %v", clusterName)

	// The NodeID for the VxFlex OS is the SdcGuid field.
	if req.GetNodeId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "The NodeID is a required field")
	}

	systemIDs := make(map[string]bool)
	systemID := req.GetArrayId()
	if systemID == "" {
		foundOne := s.getArrayIdsFromVolumes(ctx, systemIDs, req.GetVolumeIds())
		if !foundOne {
			systemID = s.defaultIsiClusterName
			systemIDs[systemID] = true
		}
	}

	// Go through each of the systemIDs
	for systemID := range systemIDs {
		log.Infof("Probe of systemID=%s", systemID)
		// Do a probe of the requested system
		//if err := s.autoProbe(ctx, isiConfig); err != nil {
		//	return nil, err
		//}

		// First - check if the node is visible from the array
		var checkError error
		checkError = s.checkIfNodeIsConnected(ctx, systemID, req.GetNodeId(), rep)
		if checkError != nil {
			return rep, checkError
		}
		log.Debugf("connction of %s to %s is %s", systemID, req.NodeId, rep.Connected)
	}
	clients, err := isiConfig.isiSvc.IsIOinProgress(ctx)
	log.Debugf("############## CLIENTS ########### %v", clients)

	log.Infof("ValidateVolumeHostConnectivity reply %+v", rep)
	return rep, nil
}

func (s *service) getArrayIdsFromVolumes(ctx context.Context, systemIDs map[string]bool, requestVolumeIds []string) bool {
	ctx, log, _ := GetRunIDLog(ctx)
	var err error
	var systemID string
	var foundAtLeastOne bool
	for _, volumeID := range requestVolumeIds {
		// Extract arrayID from the volume ID (if any volumes in the request)
		if systemID, _, _, _, err = utils.ParseNormalizedVolumeID(ctx, volumeID); err != nil {
			log.Warnf("Error getting arrayID for %s - %s", volumeID, err.Error())
		}
		if systemID != "" {
			if _, exists := systemIDs[systemID]; !exists {
				foundAtLeastOne = true
				systemIDs[systemID] = true
				log.Infof("Using systemID from %s, %s", volumeID, systemID)
			}
		} else {
			log.Infof("Could not extract systemID from %s", volumeID)
		}
	}
	return foundAtLeastOne
}

// checkIfNodeIsConnected looks at the 'nodeId' to determine if there is connectivity to the 'arrayId' array.
//The 'rep' object will be filled with the results of the check.
func (s *service) checkIfNodeIsConnected(ctx context.Context, arrayID string, nodeID string, rep *podmon.ValidateVolumeHostConnectivityResponse) error {
	ctx, log, _ := GetRunIDLog(ctx)
	log.Infof("Checking if array %s is connected to node %s", arrayID, nodeID)
	var message string
	rep.Connected = false

	nodeName, nodeFQDN, nodeIP, err := utils.ParseNodeID(ctx, nodeID)
	if err != nil {
		log.Debugf("failed to parse node ID '%s', return true for otherClientsAlreadyAdded as a safer return value", nodeID)
		return fmt.Errorf("failed to parse node ID")
	}
	log.Debugf("nodeId split to name %s, fqdn %s, IP %s", nodeName, nodeFQDN, nodeIP)

/*	log.Info("check node status")
	//form url to call array on node
	url := "http://" + nodeIP + apiPort + arrayStatus
	log.Infof("try node status with nodeIP url is --%s--", url)
	connected, err := s.queryArrayStatus(ctx, url)
	log.Debugf("query response %v, %+v", connected, err)
	if err != nil {
		message = fmt.Sprintf("node connectivity unknown due to %s", err)
		log.Info(message)
		rep.Messages = append(rep.Messages, message)
		log.Errorf(err.Error())
	}*/

	//form url to call array on node
	url := "http://" + nodeIP + apiPort + arrayStatus + "/" + arrayID
	log.Infof("try with nodeIP url is --%s--", url)
	connected, err := s.queryArrayStatus(ctx, url)
	log.Debugf("query response %v, %+v", connected, err)
	if err != nil {
		message = fmt.Sprintf("connectivity unknown for array %s to node %s due to %s", arrayID, nodeID, err)
		log.Info(message)
		rep.Messages = append(rep.Messages, message)
		log.Errorf(err.Error())
	}

	if err != nil {
		//form url to call array on node
		url = "http://" + nodeFQDN + apiPort + arrayStatus + "/" + arrayID
		log.Info("try with nodeFQDN")
		connected, err = s.queryArrayStatus(ctx, url)
		log.Debugf("query response %v, %+v", connected, err)
		if err != nil {
			message = fmt.Sprintf("connectivity unknown for array %s to node %s due to %s", arrayID, nodeID, err)
			log.Info(message)
			rep.Messages = append(rep.Messages, message)
			log.Errorf(err.Error())
		}
	}

	if err != nil {
		//form url to call array on node
		url = "http://" + nodeName + apiPort + arrayStatus + "/" + arrayID
		log.Info("try with nodename")
		connected, err = s.queryArrayStatus(ctx, url)
		log.Debugf("query response %v, %+v", connected, err)
		if err != nil {
			message = fmt.Sprintf("connectivity unknown for array %s to node %s due to %s", arrayID, nodeID, err)
			log.Info(message)
			rep.Messages = append(rep.Messages, message)
			log.Errorf(err.Error())
			return err
		}
	}

	if connected {
		message = fmt.Sprintf("array %s is connected to node %s", arrayID, nodeID)
	} else {
		message = fmt.Sprintf("array %s is not connected to node %s", arrayID, nodeID)
	}
	log.Info(message)
	rep.Connected = true
	rep.Messages = append(rep.Messages, message)
	return nil
}
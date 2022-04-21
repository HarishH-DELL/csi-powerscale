package service

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/dell/csi-isilon/common/constants"
	csictx "github.com/dell/gocsi/context"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	nodeStatus = "/node-status"
	arrayStatus = "/array-status"
)

// pollingFrequency in seconds
var pollingFrequencyInSeconds int64

var apiPort = ":8083"

// probeStatus map[string]ArrayConnectivityStatus
var probeStatus *sync.Map

//ArrayConnectivityStatus Status of the array probe
type ArrayConnectivityStatus struct {
	LastSuccess int64 `json:"lastSuccess"`  // only succeeded , not connected if we missed a reply. 2-3 missed cycles is a disconnection
	LastAttempt int64 `json:"lastAttempt"`  // every 60s irrespective of response, decide to 30s
}

func (s *service) setAPIPort(ctx context.Context) {
	ctx, log := GetLogger(ctx)
	if port, ok := csictx.LookupEnv(ctx, constants.EnvPodmonAPIPORT); ok {
		log.Debugf("set podmon API port to %s", apiPort)
		apiPort = ":"+port
	} else {
		// If the port number cannot be fetched, set it to default
		apiPort = ":"+constants.DefaultPodmonAPIPortNumber
		log.Debugf("set podmon API port to default %s", apiPort)
	}
}

// MarshalSyncMapToJSON marshal the sync Map to Json
func MarshalSyncMapToJSON(m *sync.Map) ([]byte, error) {
	tmpMap := make(map[string]ArrayConnectivityStatus)
	m.Range(func(k, v interface{}) bool {
		tmpMap[k.(string)] = v.(ArrayConnectivityStatus)
		return true
	})
	return json.Marshal(tmpMap)
}

//StartAPIService reads nodes to array status periodically
func (s *service) StartAPIService(ctx context.Context) {
	//Fetch the arrayConnectivityPollRate of podmon and set half of it as pollingFrequency subject to minimum of 30
	arrayConnectivityPollRate := flag.Int("arrayConnectivityPollRate", 30, "time in seconds to poll array")
	log.Debugf("fetch arrayConnectivityPollRate %d", arrayConnectivityPollRate)
	pollingFrequency := *arrayConnectivityPollRate/2
	if pollingFrequency <30 {
		pollingFrequency =30
	}
	log.Debugf("set pollingFrequency as %d seconds", pollingFrequency)
	s.startNodeToArrayConnectivityCheck(ctx)
	//start methods based on mode
	if strings.EqualFold(s.mode, constants.ModeNode) {
		s.apiRouter(ctx)
	}
}

//apiRouter serves http requests
func (s *service) apiRouter(ctx context.Context) {
	log.Infof("starting http server on port %s", apiPort)
	//create a new router
	router := mux.NewRouter()
	//route to connectivity status
	router.HandleFunc(nodeStatus, nodeHealth).Methods("GET")
	router.HandleFunc(arrayStatus, connectivityStatus).Methods("GET")
	router.HandleFunc(arrayStatus+"/"+"{arrayId}", getArrayConnectivityStatus).Methods("GET")
	//start http server to serve requests
	http.ListenAndServe(apiPort, router)
}

// GetArrayConnectivityStatus lists status of the requested array
func getArrayConnectivityStatus(w http.ResponseWriter, r *http.Request) {
	arrayID := mux.Vars(r)["arrayId"]
	log.Infof("GetArrayConnectivityStatus called for array %s \n", arrayID)
	status, found := probeStatus.Load(arrayID)
	if !found {
		//specify status code
		w.WriteHeader(http.StatusNotFound)
		w.Header().Set("Content-Type", "application/json")
		//update response writer
		fmt.Fprintf(w, "array %s not found \n", arrayID)
		return
	}
	//convert struct to JSON
	jsonResponse, err := json.Marshal(status)
	if err != nil {
		log.Errorf("error %s during marshaling to json", err)
		return
	}
	//update response
	w.WriteHeader(http.StatusFound)
	w.Write(jsonResponse)
}

// NodeHealth states if node is up
func nodeHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "node is up and running \n")
}

// ConnectivityStatus Returns array connectivity status
func connectivityStatus(w http.ResponseWriter, r *http.Request) {
	log.Infof("ConnectivityStatus called, urr status is %v \n", probeStatus)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusFound)

	//convert struct to JSON
	jsonResponse, err := MarshalSyncMapToJSON(probeStatus)
	if err != nil {
		log.Errorf("error %s during marshaling to json", err)
		return
	}
	w.Write(jsonResponse)
}

//startNodeToArrayConnectivityCheck runs probe to test connectivity from node to array
//updates probeStatus map[array]ArrayConnectivityStatus
func (s *service) startNodeToArrayConnectivityCheck(ctx context.Context) error {
	log.Info("startNodeToArrayConnectivityCheck called")
	probeStatus = new(sync.Map)
	var status ArrayConnectivityStatus
	ctx, log := GetLogger(ctx)

	//TODO: Choose to have goroutines for every array so that each runs independently and isnt blocked by others.
	go func() {
		for range time.Tick(time.Second * time.Duration(pollingFrequencyInSeconds)) {
			log.Debugf("Running probe for all clusters at time %v \n", time.Now())
			isilonClusters := s.getIsilonClusters()
			for i := range isilonClusters {
				if status, ok := probeStatus.Load(isilonClusters[i].ClusterName); !ok {
					log.Debugf("%s not in probeStatus ", isilonClusters[i].ClusterName)
					probeStatus.Store(isilonClusters[i].ClusterName, status)
				}
				err := s.nodeProbe(ctx, isilonClusters[i])
				if err == nil {
					log.Debugf("Probe successful for %s", isilonClusters[i].ClusterName)
					status.LastSuccess = time.Now().Unix()
				} else {
					log.Debugf("Probe failed for isilon cluster '%s' error:'%s'", isilonClusters[i].ClusterName, err)
				}
				status.LastAttempt = time.Now().Unix()
				probeStatus.Store(isilonClusters[i].ClusterName, status)
			}
			log.Infof("probeStatus is %v", &probeStatus)
		}
	}()
	return nil
}

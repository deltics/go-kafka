package kafka

type configKeyId = int

const (
	acks configKeyId = iota
	batchSize
	bootstrapServers
	clientId
	enableAutoCommit
	enableIdempotence
	groupId
	lingerMs
	maxInFlightRequestsPerConnections
	retries
)

var key = map[configKeyId]string{
	acks:                              "acks",                                  // P
	batchSize:                         "batch.size",                            // P
	bootstrapServers:                  "bootstrap.servers",                     // P, C
	clientId:                          "client.id",                             // P
	enableAutoCommit:                  "enable.auto.commit",                    // C
	enableIdempotence:                 "enable.idempotence",                    // P
	groupId:                           "group.id",                              // C
	lingerMs:                          "linger.ms",                             // P
	maxInFlightRequestsPerConnections: "max.in.flight.requests.per.connection", // P
	retries:                           "retries",                               // P, C?
}

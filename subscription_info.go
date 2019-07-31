package kaf

type SubscriptionInfo struct {
	Version      int32
	UUID         []byte // 16-byte UUID
	ProcessID    string
	PrevTasks    []TaskID
	StandbyTasks []TaskID
	UserEndpoint string
}

// Support version 1+2
func (s *SubscriptionInfo) Decode(pd PacketDecoder) (err error) {
	s.Version, err = pd.getInt32()
	if err != nil {
		return err
	}

	s.UUID, err = pd.getRawBytes(16)
	if err != nil {
		return err
	}

	numPrevs, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := 0; i < int(numPrevs); i++ {
		t := TaskID{}

		t.TopicGroupID, err = pd.getInt32()
		if err != nil {
			return err
		}

		t.Partition, err = pd.getInt32()
		if err != nil {
			return err
		}

		s.PrevTasks = append(s.PrevTasks, t)
	}

	numStandby, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := 0; i < int(numStandby); i++ {
		t := TaskID{}

		t.TopicGroupID, err = pd.getInt32()
		if err != nil {
			return err
		}

		t.Partition, err = pd.getInt32()
		if err != nil {
			return err
		}

		s.StandbyTasks = append(s.StandbyTasks, t)
	}

	userEndpointBytes, err := pd.getBytes()
	if err != nil {
		return err
	}

	s.UserEndpoint = string(userEndpointBytes)

	return nil
}

type TaskID struct {
	TopicGroupID int32
	Partition    int32
}

package gemini

import (
	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"

	"fmt"
)

type Session struct {
	testSession   *gocql.Session
	oracleSession *gocql.Session
}

func NewSession(testClusterHost string, oracleClusterHost string) *Session {
	testCluster := gocql.NewCluster(testClusterHost)
	testSession, err := testCluster.CreateSession()
	if err != nil {
		panic(err)
	}

	oracleCluster := gocql.NewCluster(oracleClusterHost)
	oracleSession, err := oracleCluster.CreateSession()
	if err != nil {
		panic(err)
	}

	return &Session{
		testSession:   testSession,
		oracleSession: oracleSession,
	}
}

func (s *Session) Close() {
	s.testSession.Close()
	s.oracleSession.Close()
}

func (s *Session) Mutate(query string) error {
	if err := s.testSession.Query(query).Exec(); err != nil {
		return fmt.Errorf("%v [cluster = test, query = '%s']", err, query)
	}
	if err := s.oracleSession.Query(query).Exec(); err != nil {
		return fmt.Errorf("%v [cluster = oracle, query = '%s']", err, query)
	}
	return nil
}

func (s *Session) Check(query string) string {
	testIter := s.testSession.Query(query).Iter()
	oracleIter := s.oracleSession.Query(query).Iter()
	for {
		testRow := make(map[string]interface{})
		if !testIter.MapScan(testRow) {
			break
		}
		oracleRow := make(map[string]interface{})
		if !oracleIter.MapScan(oracleRow) {
			break
		}
		if diff := cmp.Diff(oracleRow, testRow); diff != "" {
			return diff
		}
	}
	return ""
}

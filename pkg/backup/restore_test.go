package backup

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDetectRBACObject(t *testing.T) {
	b := &Backuper{} // Create an instance of Backuper for testing

	testCases := []struct {
		inputSQL     string
		expectedKind string
		expectedName string
		expectedErr  error
	}{
		{
			inputSQL:     "ATTACH ROLE `admin`",
			expectedKind: "ROLE",
			expectedName: "admin",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH USER `user1` WITH PASSWORD 'password'",
			expectedKind: "USER",
			expectedName: "user1",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH ROLE test_rbac SETTINGS PROFILE ID('4949fb42-97bb-4841-4b5b-c05d4b0cb685');\n",
			expectedKind: "ROLE",
			expectedName: "test_rbac",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH ROW POLICY `test_rbac` ON default.test_rbac AS restrictive FOR SELECT USING 1 = 1 TO ID('e1469fb8-e014-c22b-4e5c-406134320f91');\n",
			expectedKind: "ROW POLICY",
			expectedName: "`test_rbac` ON default.test_rbac",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH SETTINGS PROFILE `test_rbac` SETTINGS max_execution_time = 60.;\n",
			expectedKind: "SETTINGS PROFILE",
			expectedName: "test_rbac",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH QUOTA test_rbac KEYED BY user_name TO ID('e1469fb8-e014-c22b-4e5c-406134320f91');\n",
			expectedKind: "QUOTA",
			expectedName: "test_rbac",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH USER test_rbac IDENTIFIED WITH sha256_hash BY '256A6D6B157C014A70BE5C62ACA0FE4A6183BFBD45895F62287447B55E519BAD' DEFAULT ROLE ID('2d449952-fca4-c9f2-2949-b83880124bbc');\nATTACH GRANT ID('2d449952-fca4-c9f2-2949-b83880124bbc') TO test_rbac;\n",
			expectedKind: "USER",
			expectedName: "test_rbac",
			expectedErr:  nil,
		},
		{
			inputSQL:     "INVALID SQL",
			expectedKind: "",
			expectedName: "",
			expectedErr:  fmt.Errorf("unable to detect RBAC object kind from SQL query: INVALID SQL"),
		},
		{
			inputSQL:     "ATTACH USER  ",
			expectedKind: "USER",
			expectedName: "",
			expectedErr:  fmt.Errorf("unable to detect RBAC object name from SQL query: ATTACH USER  "),
		},
	}

	for _, tc := range testCases {
		kind, name, err := b.detectRBACObject(tc.inputSQL)
		assert.Equal(t, tc.expectedKind, kind)
		assert.Equal(t, tc.expectedName, name)
		if tc.expectedName != "" && tc.expectedKind != "" {
			assert.NoError(t, err)
		}
		if err != nil {
			assert.Equal(t, tc.expectedErr.Error(), err.Error())
		}
	}
}

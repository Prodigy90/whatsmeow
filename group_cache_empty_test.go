package whatsmeow

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestCacheGroupInfo_SkipsGroupWithoutMembers(t *testing.T) {
	cli := &Client{groupCache: make(map[types.JID]*groupMetaCache)}
	empty := types.NewJID("120363000000000001", types.GroupServer)
	cli.cacheGroupInfo(&types.GroupInfo{JID: empty}, true)
	if _, ok := cli.groupCache[empty]; ok {
		t.Fatal("a group with no members listed must not be cached (send would address nobody, ack 479)")
	}

	full := types.NewJID("120363000000000002", types.GroupServer)
	lid := types.NewJID("111111111111111", types.HiddenUserServer)
	pn := types.NewJID("2348011111111", types.DefaultUserServer)
	pairs, _ := cli.cacheGroupInfo(&types.GroupInfo{JID: full, Participants: []types.GroupParticipant{
		{JID: lid, LID: lid, PhoneNumber: pn},
	}}, true)
	if c, ok := cli.groupCache[full]; !ok || len(c.Members) != 1 {
		t.Fatalf("a group with members must be cached, got %+v", c)
	}
	if len(pairs) != 1 || pairs[0].LID != lid || pairs[0].PN != pn {
		t.Fatalf("participant LID↔PN pair not returned: %v", pairs)
	}
}

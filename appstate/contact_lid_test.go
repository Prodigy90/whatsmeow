package appstate

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestBuildContactWithLIDCarriesLID(t *testing.T) {
	pn := types.NewJID("19472705982", types.DefaultUserServer)
	lid := types.NewJID("123966379274271", types.HiddenUserServer)
	lid.Device = 7 // a device-qualified LID must be sent as the user JID

	act := BuildContactWithLID(pn, lid, "Tttt VIP", true).Mutations[0]
	if got := act.Index[1]; got != "19472705982@s.whatsapp.net" {
		t.Fatalf("index must stay the phone-number JID, got %q", got)
	}
	if got := act.Value.GetContactAction().GetLidJID(); got != "123966379274271@lid" {
		t.Fatalf("lidJid = %q, want 123966379274271@lid", got)
	}
	if act.Value.GetContactAction().GetFullName() != "Tttt VIP" {
		t.Fatal("full name lost")
	}
}

func TestBuildContactWithLIDOmitsLIDWhenAbsentOrInvalid(t *testing.T) {
	pn := types.NewJID("19472705982", types.DefaultUserServer)
	cases := map[string]PatchInfo{
		"no lid":           BuildContactWithLID(pn, types.EmptyJID, "X", true),
		"plain wrapper":    BuildContact(pn, "X", true),
		"pn passed as lid": BuildContactWithLID(pn, pn, "X", true),
		"remove":           BuildContactWithLID(pn, types.NewJID("1", types.HiddenUserServer), "X", false),
	}
	for name, p := range cases {
		if p.Mutations[0].Value.GetContactAction().LidJID != nil {
			t.Errorf("%s: lidJid must be omitted", name)
		}
	}
}

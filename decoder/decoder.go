package decoder

import "repo.nusatek.id/sugeng/walstreamer/model"

type Decoder interface {
	Decode([]byte) (*model.Message, error)
}

package decoder

import "github.com/wongpinter/walstreamer/model"

type Decoder interface {
	Decode([]byte) (*model.Message, error)
}

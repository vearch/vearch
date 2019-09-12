//// +build vector

package ps

import (
	//if not need support vector go build --tags=vector
	_ "github.com/vearch/vearch/ps/engine/gammacb"
)

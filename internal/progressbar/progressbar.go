package progressbar

import (
	"fmt"
	"io"

	progressbar "gopkg.in/cheggaaa/pb.v1"
)

type Bar struct {
	pb   *progressbar.ProgressBar
	show bool
}

func StartNewByteBar(show bool, total int64) *Bar {
	if show {
		return &Bar{
			show: true,
			pb:   progressbar.StartNew(int(total)).SetUnits(progressbar.U_BYTES),
		}
	}
	return &Bar{
		show: false,
	}
}

func StartNewBar(show bool, total int) *Bar {
	if show {
		return &Bar{
			show: true,
			pb:   progressbar.StartNew(total),
		}
	}
	return &Bar{
		show: false,
	}
}

func (b *Bar) Finish() {
	if b.show {
		b.pb.Finish()
		fmt.Print("\033[A") // move the cursor up
	}
}

func (b *Bar) Add64(add int64) {
	if b.show {
		b.pb.Add64(add)
	}
}

func (b *Bar) Set(current int) {
	if b.show {
		b.pb.Set(current)
	}
}

func (b *Bar) Increment() {
	if b.show {
		b.pb.Increment()
	}
}

func (b *Bar) NewProxyReader(r io.Reader) io.Reader {
	if b.show {
		return b.pb.NewProxyReader(r)
	}
	return r
}

package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockid   string
	clientid string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lockid: l, clientid: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockid)
		if err != rpc.OK && err != rpc.ErrNoKey {
			continue
		} else {
			if value == "" {
				Err := lk.ck.Put(lk.lockid, lk.clientid, version)
				if Err == rpc.OK {
					return
				}
			}
			time.Sleep(time.Millisecond * 100)
		}
	}

}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.lockid)
		if err != rpc.OK {
			continue
		} else {
			if value == lk.clientid {
				Err := lk.ck.Put(lk.lockid, "", version) //锁的实现保证了释放锁时只有一个线程可以重复
				//执行put操作因此对于errversion问题只可能是锁自身的重复put导致的，所以返回errmaybe意味着
				//锁释放操作已经成功执行
				if Err == rpc.OK || Err == rpc.ErrMaybe {
					return
				}
			} else {
				return
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
	// Your code here
}

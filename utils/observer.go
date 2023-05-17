package utils

import (
	"fmt"
	"sync"
)

type Subject interface {
	Attach(o Observer)
	Detach(o Observer)
	Notify(info any)
}

type Observer interface {
	Update(info any)
}

type SubjectImpl struct {
	observers []Observer
	lk        sync.RWMutex
}

func NewSubject() Subject {
	return &SubjectImpl{
		observers: make([]Observer, 0),
	}
}
func (s *SubjectImpl) Attach(ob Observer) {
	s.lk.Lock()
	defer s.lk.Unlock()
	s.observers = append(s.observers, ob)
}

func (s *SubjectImpl) Detach(ob Observer) {
	s.lk.Lock()
	defer s.lk.Unlock()
	for i, o := range s.observers {
		if o == ob {
			s.observers = append(s.observers[:i], s.observers[i+1:]...)
			break
		}
	}
}

func (s *SubjectImpl) Notify(info any) {
	s.lk.RLock()
	defer s.lk.RUnlock()
	for _, observer := range s.observers {
		observer.Update(info)
	}
}

type ObserverImpl struct {
	subject *SubjectImpl
}

func (o *ObserverImpl) Update(info any) {
	fmt.Println("Observer updated")
}

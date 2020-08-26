package mysql2elasticsearch

import (
	"container/list"
	"fmt"
)

type Queue struct {
	queue *list.List
}

func NewQueue() *Queue {
	q := new(Queue)
	q.queue = new(list.List)
	q.queue.Init()
	return q
}

func (c *Queue) Enqueue(value interface{}) {
	c.queue.PushBack(value)
}

func (c *Queue) Dequeue() (interface{}, error) {
	if c.queue.Len() > 0 {
		ele := c.queue.Front()
		c.queue.Remove(ele)
		return ele.Value, nil
	}
	return nil, fmt.Errorf("empty")
}

func (c *Queue) Size() int {
	return c.queue.Len()
}

func (c *Queue) Empty() bool {
	return c.queue.Len() == 0
}

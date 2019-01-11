package main

import (
	"github.com/coveo/gotemplate/collections"
)

// Statistic retains statistics regarding an object category
type Statistic struct {
	Name  string
	count int
	total int
}

// AddValue adds a single value to statistics
func (s *Statistic) AddValue(value int) { s.AddValues(1, value) }

// Count returns the total number of values
func (s *Statistic) Count() int { return s.count }

// Sum returns the sum of all values
func (s *Statistic) Sum() int { return s.total }

// AddValues adds a many values to statistics
func (s *Statistic) AddValues(count, value int) {
	s.count += count
	s.total += value
}

// Average returns the average value
func (s Statistic) Average() float64 {
	if s.count == 0 {
		return 0
	}
	return float64(s.total) / float64(s.count)
}

// Statistics cumulate stats classified by specific criteria
type Statistics struct {
	index map[string]*Statistic
	List  []*Statistic
}

// AddValue adds a single values to the statistic list
func (s *Statistics) AddValue(key string, value int) { s.AddValues(key, 1, value) }

// AddValues adds a values to the statistic list
func (s *Statistics) AddValues(key string, count, value int) {
	var exist bool
	var stat *Statistic

	if stat, exist = s.index[key]; !exist {
		stat = &Statistic{Name: key}
		if s.index == nil {
			s.index = make(map[string]*Statistic)
		}
		s.index[key] = stat
		s.List = append(s.List, stat)
	}
	stat.AddValues(count, value)
}

// GetStats returns a generic list representing the statistics
func (s *Statistics) GetStats() collections.IGenericList {
	result := collections.CreateList(len(s.List))
	for i := range s.List {
		result.Set(i, map[string]interface{}{
			"Name":    s.List[i].Name,
			"Count":   s.List[i].Count(),
			"Size":    s.List[i].Sum(),
			"Average": int(s.List[i].Average()),
		})
	}
	return result
}

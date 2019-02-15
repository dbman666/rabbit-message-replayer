package main

import (
	"fmt"
	"strconv"

	"github.com/coveo/gotemplate/collections"
)

// Statistic retains statistics regarding an object category
type Statistic struct {
	Name     string
	count    int
	messages int
	sum      float64
	min, max *float64
}

// Sum returns the sum of all values
func (s *Statistic) Sum() float64 { return s.sum }

// Add adds a single value to statistics
func (s *Statistic) Add(data interface{}) Statistic {
	var value float64
	switch v := data.(type) {
	case float64:
		value = v
	case int:
		value = float64(v)
	default:
		value = must(strconv.ParseFloat(fmt.Sprint(v), 64)).(float64)
	}
	s.Join(Statistic{sum: value, messages: 1, count: 1})
	return *s
}

// Count returns the number of values considered (may be different from values)
func (s *Statistic) Count() int { return iif(s.count <= 1, 1, s.count).(int) }

// Messages returns the total number of messages
func (s *Statistic) Messages() int { return s.messages }

// Minimum returns the minimum of all values
func (s *Statistic) Minimum() float64 {
	if s.min == nil {
		return s.sum
	}
	return *s.min
}

// Maximum returns the maximum of all values
func (s *Statistic) Maximum() float64 {
	if s.max == nil {
		return s.sum
	}
	return *s.max
}

// Average returns the average value
func (s Statistic) Average() float64 {
	return s.sum / float64(s.Count())
}

// Join adds other stattistics to the current
func (s *Statistic) Join(other Statistic) Statistic {
	if s.count == 0 || other.Minimum() < s.Minimum() {
		value := other.Minimum()
		s.min = &value
	}
	if s.count == 0 || other.Maximum() > s.Maximum() {
		value := other.Maximum()
		s.max = &value
	}
	s.sum += other.sum
	s.count += other.Count()
	s.messages += other.Messages()
	return *s
}

// Statistics cumulate stats classified by specific criteria
type Statistics struct {
	index map[string]*Statistic
	List  []*Statistic
}

// Add statistic to the current statistic list
func (cum *Statistics) Add(name string, data interface{}) {
	stat := Statistic{Name: name}
	cum.AddStatistic(stat.Add(data))
}

// AddGroup to the current statistic list
func (cum *Statistics) AddGroup(name string, stat Statistic) {
	cum.AddStatistic(Statistic{Name: name, messages: stat.messages, sum: stat.sum})
}

// AddStatistic add statistics to the current statistic list
func (cum *Statistics) AddStatistic(s Statistic) {
	var exist bool
	var stat *Statistic

	if stat, exist = cum.index[s.Name]; !exist {
		stat = &Statistic{Name: s.Name}
		if cum.index == nil {
			cum.index = make(map[string]*Statistic)
		}
		cum.index[stat.Name] = stat
		cum.List = append(cum.List, stat)
	}
	stat.Join(s)
}

// Join a statistic list to the current list
func (cum *Statistics) Join(list Statistics) {
	for _, s := range list.List {
		cum.AddStatistic(*s)
	}
}

// GetStats returns a generic list representing the statistics
func (cum *Statistics) GetStats() collections.IGenericList {
	result := collections.CreateList(len(cum.List))
	for i := range cum.List {
		result.Set(i, map[string]interface{}{
			"Name":     cum.List[i].Name,
			"Count":    cum.List[i].Count(),
			"Messages": cum.List[i].Messages(),
			"Size":     cum.List[i].Sum(),
			"Average":  int(cum.List[i].Average()),
			"Minimum":  int(cum.List[i].Minimum()),
			"Maximum":  int(cum.List[i].Maximum()),
		})
	}
	return result
}

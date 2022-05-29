package main

import (
	"regexp"
	"strconv"
)

type Index struct {
	maps map[string]map[string][]IndexElement
}

type keyOperation func(string)

type IndexElement struct {
	value string
}

func NewIndex() *Index {
	index := new(Index)

	index.maps = make(map[string]map[string][]IndexElement)

	return index
}

func NewIndexElement(value string) *IndexElement {
	indexElement := new(IndexElement)

	indexElement.value = value

	return indexElement
}

func (index Index) getMap(field string) map[string][]IndexElement {
	if index.maps[field] == nil {
		index.maps[field] = make(map[string][]IndexElement)
	}

	return index.maps[field]
}

func getIndexElements(m map[string][]IndexElement, key string) []IndexElement {
	if m[key] == nil {
		var list []IndexElement
		m[key] = list
	}

	return m[key]
}

func (index Index) add(field string, key string, element IndexElement) {
	m := index.getMap(field)
	e := getIndexElements(m, key)
	m[key] = append(e, element)
}

func (index Index) remove(field string, key string, element IndexElement) {
	var elements []IndexElement

	m := index.getMap(field)
	e := getIndexElements(m, key)

	for _, ie := range e {
		if ie.value != element.value {
			elements = append(elements, element)
		}
	}

	m[key] = elements
}

func (index Index) equal(field string, key string) []IndexElement {
	m := index.getMap(field)

	return m[key]
}

func (index Index) not(field string, key string) []IndexElement {
	var results []IndexElement

	m := index.getMap(field)

	forEachKey(m, func(mapKey string) {
		if key != mapKey {
			results = append(results, m[mapKey]...)
		}
	})

	return results
}

func (index Index) match(field string, r regexp.Regexp) []IndexElement {
	var results []IndexElement

	m := index.getMap(field)

	forEachKey(m, func(mapKey string) {
		if r.MatchString(mapKey) {
			results = append(results, m[mapKey]...)
		}
	})

	return results
}

func (index Index) larger(field string, key string, parseNumber bool) []IndexElement {
	var results []IndexElement

	m := index.getMap(field)

	forEachKey(m, func(mapKey string) {
		if parseNumber {
			keyInt, _ := strconv.ParseFloat(key, 64)
			mapKeyInt, _ := strconv.ParseFloat(mapKey, 64)

			if mapKeyInt > keyInt {
				results = append(results, m[mapKey]...)
			}
		} else {
			if mapKey > key {
				results = append(results, m[mapKey]...)
			}
		}
	})

	return results
}

func (index Index) smaller(field string, key string, parseNumber bool) []IndexElement {
	var results []IndexElement

	m := index.getMap(field)

	forEachKey(m, func(mapKey string) {
		if parseNumber {
			keyInt, _ := strconv.ParseFloat(key, 64)
			mapKeyInt, _ := strconv.ParseFloat(mapKey, 64)

			if mapKeyInt < keyInt {
				results = append(results, m[mapKey]...)
			}
		} else {
			if mapKey < key {
				results = append(results, m[mapKey]...)
			}
		}
	})

	return results
}

func (index Index) between(field string, smaller string, larger string, parseNumber bool) []IndexElement {
	var results []IndexElement

	m := index.getMap(field)

	forEachKey(m, func(mapKey string) {
		if parseNumber {
			smallerInt, _ := strconv.ParseFloat(smaller, 64)
			largerInt, _ := strconv.ParseFloat(larger, 64)
			mapKeyInt, _ := strconv.ParseFloat(mapKey, 64)

			if mapKeyInt > smallerInt && mapKeyInt < largerInt {
				results = append(results, m[mapKey]...)
			}
		} else {
			if mapKey > smaller && mapKey < larger {
				results = append(results, m[mapKey]...)
			}
		}
	})

	return results
}

func forEachKey(m map[string][]IndexElement, fn keyOperation) {
	for key := range m {
		fn(key)
	}
}

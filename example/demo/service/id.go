package service

import "strconv"

func idKey(id int64) string {
	return strconv.FormatInt(id, 10)
}

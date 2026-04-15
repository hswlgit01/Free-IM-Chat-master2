package utils

func DeDuplicateSlice[T comparable](arr []T) []T {
	set := make(map[T]struct{})
	result := make([]T, 0, len(arr))

	for _, value := range arr {
		if _, exists := set[value]; !exists {
			set[value] = struct{}{}
			result = append(result, value)
		}
	}

	return result
}

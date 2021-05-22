package develop

import (
	"fmt"
	"reflect"
)

func ToString(v interface{}) string {
	return StringView(v, "", "  ")
}

func StringView(v interface{}, prefix string, indent string) string {

	if v == nil {
		return prefix + "null"
	}
	return StringViewInt(reflect.ValueOf(v), prefix, indent)
}

func StringViewInt(rv reflect.Value, prefix string, indent string) string {

	if len(prefix) > 100 {
		return "...overflow..."
	}

	if rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return "null"
		}
	}

	t := rv.Type()

	switch t.Kind() {
	case reflect.Bool:
		return fmt.Sprint(rv.Bool())
	case reflect.Float64, reflect.Float32:
		return fmt.Sprint(rv.Float())
	case reflect.Int32, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Int:
		return fmt.Sprint(rv.Int())
	case reflect.String:
		return fmt.Sprint("\"", rv.String(), "\"")
	case reflect.Chan:
		return "chan"
	case reflect.Func:
		return "func()"
	case reflect.Interface:
		return "interface{" + StringViewInt(rv.Elem(), prefix, indent) + "}"
	case reflect.Ptr:
		if rv.IsNil() {
			return "null"
		}
		sOut := "*" + StringViewInt(rv.Elem(), prefix+indent, indent)
		return sOut
	case reflect.UnsafePointer:
		return "us_ptr"
	case reflect.Map:

		sOut := "{"
		for i, k := range rv.MapKeys() {
			if i > 0 {
				sOut += ","
			}
			sOut += "\r\n" + prefix + indent
			sOut += StringViewInt(k, prefix+indent, indent)
			sOut += ":"
			sOut += StringViewInt(rv.MapIndex(k), prefix+indent, indent)
		}
		sOut += "\r\n" + prefix + "}"

		return sOut
	case reflect.Slice:

		sOut := "["
		for i := 0; i < rv.Len(); i++ {
			if i > 0 {
				sOut += ","
			}
			sOut += "\r\n" + prefix + indent
			sOut += StringViewInt(rv.Index(i), prefix+indent, indent)
		}
		sOut += "\r\n" + prefix + "]"

		return sOut
	case reflect.Struct:

		sOut := t.PkgPath() + "." + t.Name() + "{"

		for i := 0; i < rv.NumField(); i++ {
			if i > 0 {
				sOut += ","
			}
			sOut += "\r\n" + prefix + indent
			sOut += t.Field(i).Name
			sOut += ":"
			sOut += StringViewInt(rv.Field(i), prefix+indent, indent)
		}
		sOut += "\r\n" + prefix + "}"

		return sOut

	default:
		return fmt.Sprint(t.Kind(), ": \"", rv, "\"")
	}

}

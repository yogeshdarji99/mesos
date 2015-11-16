// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __STOUT_JPC__
#define __STOUT_JPC__

#include <cstddef>
#include <ostream>
#include <tuple>
#include <type_traits>
#include <utility>

#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>

#include <stout/protobuf.hpp>

#define RETURN(...) -> decltype(__VA_ARGS__) { return __VA_ARGS__; }

namespace JPC {

namespace detail {

namespace adl {

using std::begin;
using std::end;

template <typename T>
auto adl_begin(T&& t) RETURN(begin(std::forward<T>(t)))

template <typename T>
auto adl_end(T&& t) RETURN(end(std::forward<T>(t)))

} // namespace adl {

template <typename T>
struct id
{
  using type = T;
};

template <typename T, T... Is>
struct integer_sequence
{
  typedef T value_type;
  static constexpr std::size_t size() noexcept { return sizeof...(Is); }
};

template <std::size_t... Is>
using index_sequence = integer_sequence<std::size_t, Is...>;

// Glue two sets of integer_sequence together
template <typename I1, typename I2, typename I3>
struct integer_sequence_cat;

template <typename T, T... N1, T... N2, T... N3>
struct integer_sequence_cat<
    integer_sequence<T, N1...>,
    integer_sequence<T, N2...>,
    integer_sequence<T, N3...>>
{
  using type = integer_sequence<T,
      N1...,
      (sizeof...(N1) + N2)...,
      (sizeof...(N1) + sizeof...(N2) + N3)...>;
};

template <typename T, std::size_t N>
struct make_integer_sequence_impl
  : integer_sequence_cat<
        typename make_integer_sequence_impl<T, N / 2>::type,
        typename make_integer_sequence_impl<T, N / 2>::type,
        typename make_integer_sequence_impl<T, N % 2>::type> {};

template <typename T>
struct make_integer_sequence_impl<T, 0>
{
  using type = integer_sequence<T>;
};

template <typename T>
struct make_integer_sequence_impl<T, 1> {
  using type = integer_sequence<T, 0>;
};

template <typename T, T N>
using make_integer_sequence =
  typename make_integer_sequence_impl<T, static_cast<std::size_t>(N)>::type;

template <std::size_t N>
using make_index_sequence = make_integer_sequence<std::size_t, N>;

template <typename F, typename... As>
constexpr auto invoke(F &&f, As &&... as)
  RETURN(std::forward<F>(f)(std::forward<As>(as)...))

template <typename B, typename T, typename D>
constexpr auto invoke(T B::*pmv, D &&d)
  RETURN(std::forward<D>(d).*pmv)

template <typename Pmv, typename Ptr>
constexpr auto invoke(Pmv pmv, Ptr &&ptr)
  RETURN((*std::forward<Ptr>(ptr)).*pmv)

template <typename B, typename T, typename D, typename... As>
constexpr auto invoke(T B::*pmf, D &&d, As &&... as)
  RETURN((std::forward<D>(d).*pmf)(std::forward<As>(as)...))

template <typename Pmf, typename Ptr, typename... As>
constexpr auto invoke(Pmf pmf, Ptr &&ptr, As &&... as)
  RETURN(((*std::forward<Ptr>(ptr)).*pmf)(std::forward<As>(as)...))

template <typename F, typename Tuple, size_t... Is>
auto apply_impl(F&& f, Tuple&& tuple, index_sequence<Is...>)
  RETURN(invoke(std::forward<F>(f),
                std::get<Is>(std::forward<Tuple>(tuple))...));

template <typename F, typename Tuple>
auto apply(F&& f, Tuple&& tuple)
  RETURN(apply_impl(
      std::forward<F>(f),
      std::forward<Tuple>(tuple),
      make_index_sequence<
          std::tuple_size<typename std::decay<Tuple>::type>::value>{}))


// Forward declarations.
struct Boolean;
struct Number;
struct String;
template <typename Schema> struct Array;
template <typename Schema, typename F> struct Field;
template <typename Schema> struct Optional;
template <typename T, typename... Fields> struct Object;
template <typename Schema, typename F> struct Projection;
template <typename T> struct DynamicObject;
struct Protobuf;


// Proxy
template <typename Schema, typename T, typename Enable = void>
struct Proxy;


template <>
struct Proxy<Boolean, bool>
{
  bool value_;

private:
  Proxy(const Proxy&) = default;
  Proxy(Proxy&&) = default;

  friend struct Boolean;

  friend std::ostream& operator<<(std::ostream& strm, const Proxy& that)
  {
    return strm << (that.value_ ? "true" : "false");
  }
};


struct Boolean
{
  Proxy<Boolean, bool> json(bool value) const { return {value}; }
};


template <typename Integral>
struct Proxy<
    Number,
    Integral,
    typename std::enable_if<std::is_integral<Integral>::value>::type>
{
  Integral value_;

private:
  Proxy(const Proxy&) = default;
  Proxy(Proxy&&) = default;

  friend struct Number;

  friend std::ostream& operator<<(std::ostream& strm, const Proxy& that)
  {
    return strm << that.value_;
  }
};


template <typename Float>
struct Proxy<
    Number,
    Float,
    typename std::enable_if<std::is_floating_point<Float>::value>::type>
{
  Float value_;

private:
  Proxy(const Proxy&) = default;
  Proxy(Proxy&&) = default;

  friend struct Number;

  friend std::ostream& operator<<(std::ostream& strm, const Proxy& that)
  {
    // Prints a floating point value, with the specified precision, see:
    // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2006/n2005.pdf
    // Additionally ensures that a decimal point is in the output.
    char buffer[50]{}; // More than long enough for the specified precision.
    snprintf(
        buffer,
        sizeof(buffer),
        "%#.*g",
        std::numeric_limits<double>::digits10,
        that.value_);

    // Get rid of excess trailing zeroes before outputting.
    // Otherwise, printing 1.0 would result in "1.00000000000000".
    // NOTE: valid JSON numbers cannot end with a '.'.
    std::string trimmed = strings::trim(buffer, strings::SUFFIX, "0");
    return strm << trimmed << (trimmed.back() == '.' ? "0" : "");
  }
};


struct Number
{
  Proxy<Number, int32_t> json(int32_t value) const { return {value}; }
  Proxy<Number, int64_t> json(int64_t value) const { return {value}; }
  Proxy<Number, uint32_t> json(uint32_t value) const { return {value}; }
  Proxy<Number, uint64_t> json(uint64_t value) const { return {value}; }
  Proxy<Number, double> json(double value) const { return {value}; }
  Proxy<Number, float> json(float value) const { return {value}; }
};


template <>
struct Proxy<String, std::string>
{
  const std::string& value_;

private:
  Proxy(const Proxy&) = default;
  Proxy(Proxy&&) = default;

  friend struct String;

  friend std::ostream& operator<<(std::ostream& strm, const Proxy& that)
  {
    picojson::serialize_str(that.value_, std::ostreambuf_iterator<char>{strm});
    return strm;
  }
};


struct String
{
  Proxy<String, std::string> json(const std::string& value) const
  {
    return {value};
  }
};

} // namespace detail {

namespace writer {

struct Object
{
  Object(std::ostream& strm) : count_(0), strm_(strm) { strm_ << '{'; }
  ~Object() { strm_ << '}'; };

  template <typename Schema, typename T>
  void field(Schema schema, const std::string& name, const T& value)
  {
    if (count_) {
      strm_ << ',';
    }
    strm_ << detail::String{}.json(name) << ':' << schema.json(value);
    ++count_;
  }

private:
  std::size_t count_;
  std::ostream& strm_;
};

} // namespace writer {

namespace detail {

template <typename Schema, typename Iter>
struct Proxy<Array<Schema>, Iter>
{
  Schema schema_;
  Iter begin_;
  Iter end_;

private:
  Proxy(const Proxy&) = default;
  Proxy(Proxy&&) = default;

  friend struct Array<Schema>;

  friend std::ostream& operator<<(std::ostream& strm, const Proxy& that)
  {
    strm << '[';
    if (that.begin_ != that.end_) {
      auto iter = that.begin_;
      strm << that.schema_.json(*iter);
      for (++iter; iter != that.end_; ++iter) {
        strm << ',' << that.schema_.json(*iter);
      }
    }
    strm << ']';
    return strm;
  }
};


template <typename Schema>
struct Array
{
  constexpr Array(Schema schema) : schema_(std::move(schema)) {}
private:
  Schema schema_;

  template <typename Iter>
  auto json(Iter begin, Iter end) const ->
    typename decltype(begin != end, ++begin, id<Proxy<Array, Iter>>{})::type
  {
    return {schema_, begin, end};
  }

public:
  template <typename Iterable>
  auto json(const Iterable& value) const
    RETURN(this->json(adl::adl_begin(value), adl::adl_end(value)))
};


template <typename Schema, typename T>
struct Proxy<Optional<Schema>, T>
{
  Schema schema_;
  const T& value_;

private:
  Proxy(const Proxy&) = default;
  Proxy(Proxy&&) = default;

  friend struct Optional<Schema>;

  /*
  friend std::ostream& operator<<(std::ostream& strm, const Proxy& that)
  {
    write(strm, that.value_);
    if (that.value_) {
      strm << that.schema_.json(*that.value_);
    } else {
      strm << "null";
    }
    return strm;
  }
  */
};


template <typename Schema>
struct Optional
{
  constexpr Optional(Schema schema) : schema_(std::move(schema)) {}
private:
  /*
  template <typename T>
  id<Proxy<Optional, Option<T>>> json(const Option<T>& value) const
  {
    return {schema_, value};
  }

  template <typename T>
  auto json(const T& value) const -> typename decltype(
      static_cast<bool>(value), *value, id<Proxy<Optional, T>>{})::type
  {
    return {schema_, value};
  }
  */

  Schema schema_;

  template <typename, typename, typename> friend struct Proxy;
  template <typename, typename> friend struct Field;
};


template <typename Schema, typename F>
struct Field
{
  constexpr Field(Schema schema, const char* name, F f)
    : schema_(std::move(schema)), name_(std::move(name)), f_(std::move(f)) {}

private:
  template <typename T, typename S>
  void write_impl(
      std::ostream& strm,
      const T& value,
      const S& schema,
      bool leading_comma) const
  {
    if (leading_comma) {
      strm << ',';
    }
    strm << String{}.json(name_) << ':' << schema.json(value);
  }

  template <typename T, typename S>
  void write_impl(
      std::ostream& strm,
      const Option<T>& value,
      const Optional<S>& schema,
      bool leading_comma) const
  {
    if (value.isSome()) {
      write_impl(strm, value.get(), schema.schema_, leading_comma);
    }
  }

  template <typename T, typename S>
  auto write_impl(
      std::ostream& strm,
      const T& value,
      const Optional<S>& schema,
      bool leading_comma) const ->
    typename decltype(static_cast<bool>(value), *value, void())::type
  {
    if (value) {
      write_impl(strm, *value, schema.schema_, leading_comma);
    }
  }

  template <typename Object, typename S, typename G>
  auto write_dispatch(
      std::ostream& strm,
      const Object&,
      const Optional<S>& schema,
      G g,
      bool leading_comma) const -> decltype(invoke(g), void())
  {
    write_impl(strm, invoke(g), schema, leading_comma);
  }

  template <typename Object, typename S, typename G>
  auto write_dispatch(
      std::ostream& strm,
      const Object& object,
      const Optional<S>& schema,
      G g,
      bool leading_comma) const
    -> decltype(invoke(g, object), void())
  {
    write_impl(strm, invoke(g, object), schema, leading_comma);
  }

  template <typename Object, typename S, typename G>
  auto write_dispatch(
      std::ostream& strm,
      const Object&,
      const S& schema,
      G g,
      bool leading_comma) const -> decltype(invoke(g), void())
  {
    write_impl(strm, invoke(g), schema, leading_comma);
  }

  template <typename Object, typename S, typename G>
  auto write_dispatch(
      std::ostream& strm,
      const Object& object,
      const S& schema,
      G g,
      bool leading_comma) const -> decltype(invoke(g, object), void())
  {
    write_impl(strm, invoke(g, object), schema, leading_comma);
  }

  template <typename Object>
  void write(std::ostream& strm, const Object& object, bool leading_comma) const
  {
    write_dispatch(strm, object, schema_, f_, leading_comma);
  }

  Schema schema_;
  const char* name_;
  F f_;

  template <typename, typename, typename> friend struct Proxy;
};


template <typename... Fields, typename T>
struct Proxy<Object<T, Fields...>, T>
{
  std::tuple<Fields...> fields_;
  const T& value_;

private:
  struct write
  {
    template <typename Field, typename... MoreFields>
    void operator()(const Field& field, const MoreFields&... more_fields)
    {
      field.write(strm_, value_, /* leading_comma = */ false);
      int for_each[] = {
        (more_fields.write(strm_, value_, /* leading_comma = */ true), 0)...};
      static_cast<void>(for_each);
    }
    std::ostream& strm_;
    const T& value_;
  };

  Proxy(const Proxy&) = default;
  Proxy(Proxy&&) = default;

  friend struct Object<T, Fields...>;

  friend std::ostream& operator<<(std::ostream& strm, const Proxy& that)
  {
    strm << '{';
    apply(write{strm, that.value_}, that.fields_);
    strm << '}';
    return strm;
  }
};


template <typename T, typename... Fields>
struct Object
{
  template <typename... Schemas, typename... Fs>
  constexpr Object(Field<Schemas, Fs>... fields)
    : fields_{std::move(fields)...} {}

  constexpr Object(std::tuple<Fields...> fields) : fields_(std::move(fields)) {}

  Proxy<Object, T> json(const T& value) const { return {fields_, value}; }

  std::tuple<Fields...> fields() const { return fields_; }

private:
  std::tuple<Fields...> fields_;
};


template <typename Schema, typename F, typename T>
struct Proxy<Projection<Schema, F>, T>
{
  Schema schema_;
  const F& f_;
  const T& value_;

private:
  Proxy(const Proxy&) = default;
  Proxy(Proxy&&) = default;

  friend struct Array<Schema>;

  friend std::ostream& operator<<(std::ostream& strm, const Proxy& that)
  {
    strm << that.schema_.json(invoke(that.f_, that.value_));
    return strm;
  }
};


template <typename Schema, typename F>
struct Projection {
  constexpr Projection(Schema schema, F f)
    : schema_(std::move(schema)), f_(std::move(f)) {}

private:
  Schema schema_;
  F f_;

public:
  template <typename T>
  auto json(const T& value) const ->
    typename decltype(this->schema_.json(invoke(this->f_, value)),
        id<Proxy<Projection, T>>{})::type
  {
    return {schema_, f_, value};
  }
};


template <typename T>
struct Proxy<DynamicObject<T>, T>
{
  void (*f_)(JPC::writer::Object&, const T&);
  const T& value_;

private:
  Proxy(const Proxy&) = default;
  Proxy(Proxy&&) = default;

  friend struct DynamicObject<T>;

  friend std::ostream& operator<<(std::ostream& strm, const Proxy& that)
  {
    JPC::writer::Object object(strm);
    that.f_(object, that.value_);
    return strm;
  }
};


template <typename T>
struct DynamicObject
{
  constexpr DynamicObject(void (*f)(JPC::writer::Object&, const T&))
    : f_(std::move(f)) {}

  Proxy<DynamicObject, T> json(const T& value) const { return {f_, value}; }
private:
  void (*f_)(JPC::writer::Object&, const T&);
};


template <>
struct Proxy<Protobuf, google::protobuf::Message>
{
  const google::protobuf::Message& value_;

private:
  Proxy(const Proxy&) = default;
  Proxy(Proxy&&) = default;

  friend struct Protobuf;

  friend std::ostream& operator<<(std::ostream& strm, const Proxy& that)
  {
    return strm << JSON::protobuf(that.value_);
  }
};


struct Protobuf
{
  Proxy<Protobuf, google::protobuf::Message> json(
      const google::protobuf::Message& value) const
  {
    return {value};
  }
};


template <typename T, typename... LhsFields, typename... RhsFields>
Object<T, LhsFields..., RhsFields...> operator+(
    const Object<T, LhsFields...>& lhs, const Object<T, RhsFields...>& rhs)
{
  return std::tuple_cat(lhs.fields(), rhs.fields());
}


template <typename Schema, typename F>
Projection<Schema, F> compose(Schema schema, F f)
{
  return {std::move(schema), std::move(f)};
}


template <typename Schema, typename F>
auto operator<<(Schema schema, F f)
  RETURN(compose(std::move(schema), std::move(f)))


template <typename Schema, typename R, typename T>
auto operator<<(Schema schema, R (*f)(const T&))
  RETURN(compose(std::move(schema), std::move(f)))


template <typename Schema, typename R, typename T>
auto operator<<(Schema schema, R (T::*f)() const)
  RETURN(compose(std::move(schema), std::move(f)))

} // namespace detail {

constexpr detail::Boolean boolean{};
constexpr detail::Number number{};
constexpr detail::String string{};

template <typename Schema>
constexpr detail::Array<Schema> array(Schema schema)
{
  return {std::move(schema)};
}

template <typename Schema, typename F>
constexpr detail::Field<Schema, F> field(Schema schema, const char* name, F f)
{
  return {std::move(schema), std::move(name), std::move(f)};
}

template <typename Schema, typename R, typename T>
constexpr detail::Field<Schema, R (T::*)() const> field(
    Schema schema, const char* name, R (T::*f)() const)
{
  return {std::move(schema), std::move(name), std::move(f)};
}

template <typename Schema>
constexpr detail::Optional<Schema> optional(Schema schema)
{
  return {std::move(schema)};
}

template <typename T, typename... Schemas, typename... Fs>
constexpr detail::Object<T, detail::Field<Schemas, Fs>...> object(
    detail::Field<Schemas, Fs>... fields)
{
  return {std::move(fields)...};
}

template <typename T>
constexpr detail::DynamicObject<T> dynamic_object(
    void (*f)(JPC::writer::Object&, const T&))
{
  return {std::move(f)};
}

constexpr detail::Protobuf protobuf{};

} // namespace JPC {

#undef RETURN

#endif // __STOUT_JPC__

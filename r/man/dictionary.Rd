% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dictionary.R
\name{dictionary}
\alias{dictionary}
\title{dictionary type factory}
\usage{
dictionary(index_type, value_type, ordered = FALSE)
}
\arguments{
\item{index_type}{index type, e.g. \code{\link[=int32]{int32()}}}

\item{value_type}{value type, probably \code{\link[=utf8]{utf8()}}}

\item{ordered}{Is this an ordered dictionary ?}
}
\value{
a \link[=arrow__DictionaryType]{arrow::DictionaryType}
}
\description{
dictionary type factory
}

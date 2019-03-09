library("readr")
library("ChannelAttribution")
library("ggplot2")
library("reshape")
library("dplyr")
library("plyr")
library("reshape2")
library("markovchain")
library("plotly")


# Incluyendo productos como canales

modelo <- function(datos, dicc, resultado){
    canales <- readr::read_csv(dicc, col_types = readr::cols())
    result <- readr::read_csv(datos, col_types = readr::cols())
    H <- ChannelAttribution::heuristic_models(result, "path", "conversion",
                                              var_value = "value")
    M <- markov_model(result, "path", "conversion", var_value = "value",
                      order = 1, seed = 1234)
    R <- merge(H, M, by = "channel_name")
    R1 <- R[, (colnames(R) %in%
               c("channel_name",
                 "last_touch_conversions",
                 "last_touch_value",
                 "total_conversion",
                 "total_conversion_value"
             ))]
    names(R1) <- c("canal", "conversiones_last", "valor_last",
                   "conversiones_markov", "valor_markov")
    R1 <- merge(canales, R1, by = "canal")
    R1 <- R1[order(R1$conversiones_markov,
        R1$conversiones_last, decreasing = T), ]
    R1$canal <- NULL
    readr::write_csv(R1, resultado)
}

elems <- list.files(path = ".", pattern = "data.*\\.csv$")
for (elem in elems){
    result <- gsub("data", "result", elem)
    modelo(elem, "dict.csv", result)
}

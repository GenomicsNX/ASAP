### ASAP Heatmap script
options(echo=TRUE)
args <- commandArgs(trailingOnly = TRUE)

### Default Parameters
input.file <- args[1]
output.folder <- args[2]

### Libraries
require(jsonlite)
require(d3heatmap)
require(plotly)

### Functions
error.json <- function(displayed) {
  stats <- list()
  stats$displayed_error = displayed
  write(toJSON(stats, method="C", auto_unbox=T), file = paste0(output.folder,"/output.json"), append=F)
  stop(displayed)
}
plotly_json <- function(p, ...) {
  plotly:::to_JSON(plotly_build(p), ...)
}

### Read file
data.norm <- read.table(input.file, sep="\t", header=T, row.names=1, colClasses=c(Genes="character"), check.names=F, stringsAsFactors=F)
#data.norm <- read.table("C:/users/vincent gardeux/Dropbox/ASAP/Scripts/NORMALIZED.tab", sep="\t", header=T, row.names=1, colClasses=c(Genes="character"), check.names=F, stringsAsFactors=F)[1:100,]
#output.folder="C:/users/vincent gardeux/Desktop"
data.warnings <- NULL

### Heatmap
dist.method <- args[3]
if(is.na(dist.method) || dist.method == ""){
  print("No dist.method parameter. Running with euclidean.")
  dist.method <- "euclidean"
}
clust.method <- args[4]
if(is.na(clust.method) || clust.method == ""){
  print("No clust.method parameter. Running with ward.D2.")
  clust.method <- "ward.D2"
}

if(dist.method == "pearson" || dist.method == "spearman") {
  distfun <- function(x) as.dist(1 - cor(x, method = dist.method))
}else{
  distfun <- function(x) dist(x, method=dist.method)
}
hclustfun <- function(x) hclust(x, method=clust.method)

# Heatmap
h = d3heatmap(data.norm, scale = "column", hclustfun = hclustfun, distfun = distfun)
hh = d3heatmapOutput(h)
write(plotly_json(hh, pretty = TRUE), file = paste0(output.folder,"/output.heatmap.json"), append=F)

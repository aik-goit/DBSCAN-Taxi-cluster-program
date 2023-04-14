// Project CSI2120/CSI2520
// Winter 2022
// Robert Laganiere, uottawa.ca

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type GPScoord struct {
	lat  float64
	long float64
}

type LabelledGPScoord struct {
	GPScoord
	ID    int // point ID
	Label int // cluster ID
}

type sent struct {
	s      []LabelledGPScoord
	MinPts int
	eps    float64
	offset int
}

const N int = 4
const numConsumers = 4
const MinPts int = 5
const eps float64 = 0.0003
const filename string = "yellow_tripdata_2009-01-15_9h_21h_clean.csv"

func main() {

	start := time.Now()

	gps, minPt, maxPt := readCSVFile(filename)
	fmt.Printf("Number of points: %d\n", len(gps))

	minPt = GPScoord{40.7, -74.}
	maxPt = GPScoord{40.8, -73.93}

	// geographical limits
	fmt.Printf("SW:(%f , %f)\n", minPt.lat, minPt.long)
	fmt.Printf("NE:(%f , %f) \n\n", maxPt.lat, maxPt.long)

	// Parallel DBSCAN STEP 1.
	incx := (maxPt.long - minPt.long) / float64(N)
	incy := (maxPt.lat - minPt.lat) / float64(N)

	var grid [N][N][]LabelledGPScoord // a grid of GPScoord slices

	// Create the partition
	// triple loop! not very efficient, but easier to understand

	partitionSize := 0
	for j := 0; j < N; j++ {
		for i := 0; i < N; i++ {

			for _, pt := range gps {

				// is it inside the expanded grid cell
				if (pt.long >= minPt.long+float64(i)*incx-eps) && (pt.long < minPt.long+float64(i+1)*incx+eps) && (pt.lat >= minPt.lat+float64(j)*incy-eps) && (pt.lat < minPt.lat+float64(j+1)*incy+eps) {

					grid[i][j] = append(grid[i][j], pt) // add the point to this slide
					partitionSize++
				}
			}
		}
	}

	var grid2 []LabelledGPScoord
	for _, h := range grid {
		for _, i := range h {
			for _, j := range i {
				grid2 = append(grid2, j)
			}

		}

	}

	jobs := make(chan sent)

	var mutex sync.WaitGroup
	mutex.Add(numConsumers)

	go produce(grid, jobs)

	for i := 0; i < numConsumers; i++ {
		go consume(i, jobs, &mutex)
	}

	mutex.Wait()

	// Parallel DBSCAN STEP 2.
	// Apply DBSCAN on each partition
	// ...

	// Parallel DBSCAN step 3.
	// merge clusters
	// *DO NOT PROGRAM THIS STEP

	end := time.Now()
	fmt.Printf("\nExecution time: %s of %d points\n", end.Sub(start), partitionSize)
	fmt.Printf("Number of CPUs: %d", runtime.NumCPU())
}

func produce(grid [N][N][]LabelledGPScoord, jobs chan<- sent) {
	for j := 0; j < N; j++ {
		for i := 0; i < N; i++ {
			var a sent
			a.s = grid[i][j]
			a.eps = eps
			a.MinPts = MinPts
			a.offset = i*10000000 + j*1000000
			jobs <- a
		}
	}
	close(jobs)
}

func consume(worker int, jobs <-chan sent, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {

		DBscan(job.s, job.MinPts, job.eps, job.offset)
	}

}

func contains(s []LabelledGPScoord, str LabelledGPScoord) bool {
	for v := 0; v < len(s); v++ {
		if s[v] == str {
			return true
		}
	}

	return false
}

func getDistance(a LabelledGPScoord, b LabelledGPScoord) (f float64) {
	var distanceLat = a.lat - b.lat
	var distanceLon = a.long - b.long
	return math.Sqrt(distanceLon*distanceLon + distanceLat*distanceLat)
}

func rangeQuery(p LabelledGPScoord, eps float64, pickupCoords []LabelledGPScoord) (r []LabelledGPScoord) {
	var newNeighbours []LabelledGPScoord
	for i := 0; i < len(pickupCoords); i++ {
		var q = pickupCoords[i]
		if (getDistance(p, q) <= eps) && (p != q) {
			newNeighbours = append(newNeighbours, q)
		}
	}
	return newNeighbours
}

func Merge(a []LabelledGPScoord, b []LabelledGPScoord) (c []LabelledGPScoord) {
	for i := 0; i < len(b); i++ {
		var t = b[i]
		if contains(a, t) == false {
			a = append(a, t)
		}
	}
	return a
}

// Applies DBSCAN algorithm on LabelledGPScoord points
// LabelledGPScoord: the slice of LabelledGPScoord points
// MinPts, eps: parameters for the DBSCAN algorithm
// offset: label of first cluster (also used to identify the cluster)
// returns number of clusters found
func DBscan(coords []LabelledGPScoord, MinPts int, eps float64, offset int) (nclusters int) {

	// *** fake code: to be rewritten
	//time.Sleep(3)
	nclusters = 0
	//clusterindex = 0
	var visited []LabelledGPScoord
	//var pointList []LabelledGPScoord

	for index2 := 0; index2 < len(coords); index2++ {
		var p = coords[index2]
		if contains(visited, p) == false {
			visited = append(visited, p)
			var Neighbours = rangeQuery(p, eps, coords)
			if len(Neighbours) >= MinPts {
				for j := 0; j < len(Neighbours); j++ {
					var r = Neighbours[j]
					if contains(visited, r) == false {
						visited = append(visited, r)
						var Neighbours2 = rangeQuery(r, eps, coords)
						if len(Neighbours2) >= MinPts {
							//Neighbours = Merge(Neighbours, Neighbours2)
							for i := 0; i < len(Neighbours2); i++ {
								var t = Neighbours2[i]
								if contains(Neighbours, t) == false {
									Neighbours = append(Neighbours, t)
								}
							}

						}
					}
				}
				//fmt.Printf("\n", len(Neighbours))
				nclusters++
			}

		}

		//pt.Label = offset + nclusters

	}
	// *** end of fake code.

	// End of DBscan function
	// Printing the result (do not remove)
	fmt.Printf("Partition %10d : [%4d,%6d]\n", offset, nclusters, len(coords))

	return nclusters
}

// reads a csv file of trip records and returns a slice of the LabelledGPScoord of the pickup locations
// and the minimum and maximum GPS coordinates
func readCSVFile(filename string) (coords []LabelledGPScoord, minPt GPScoord, maxPt GPScoord) {

	coords = make([]LabelledGPScoord, 0, 5000)

	// open csv file
	src, err := os.Open(filename)
	defer src.Close()
	if err != nil {
		panic("File not found...")
	}

	// read and skip first line
	r := csv.NewReader(src)
	record, err := r.Read()
	if err != nil {
		panic("Empty file...")
	}

	minPt.long = 1000000.
	minPt.lat = 1000000.
	maxPt.long = -1000000.
	maxPt.lat = -1000000.

	var n int = 0

	for {
		// read line
		record, err = r.Read()

		// end of file?
		if err == io.EOF {
			break
		}

		if err != nil {
			panic("Invalid file format...")
		}

		// get lattitude
		lat, err := strconv.ParseFloat(record[9], 64)
		if err != nil {
			panic("Data format error (lat)...")
		}

		// is corner point?
		if lat > maxPt.lat {
			maxPt.lat = lat
		}
		if lat < minPt.lat {
			minPt.lat = lat
		}

		// get longitude
		long, err := strconv.ParseFloat(record[8], 64)
		if err != nil {
			panic("Data format error (long)...")
		}

		// is corner point?
		if long > maxPt.long {
			maxPt.long = long
		}

		if long < minPt.long {
			minPt.long = long
		}

		// add point to the slice
		n++
		pt := GPScoord{lat, long}
		coords = append(coords, LabelledGPScoord{pt, n, 0})
	}

	return coords, minPt, maxPt
}

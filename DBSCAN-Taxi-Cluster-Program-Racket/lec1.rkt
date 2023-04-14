#lang racket
(require racket/trace)

(define (lowest-exponent B T )
 (lowest-exponenth B B T))

(define (lowest-exponenth Z B T )
  (if (>= B T)
      1
      (+ 1 (lowest-exponenth Z (* B Z) T))))


(define (find-abundant N)
 (list-abundant N '()))

(define (list-abundant N L)
  (cond [(= N 0) (list L)]
        [(>(sum-div N N)(* N 2))(set! L (append L (list N)))(list-abundant (- N 1) L)]
        [else (list-abundant(sub1 N) L)]))

(define (test L)
  (list L))

(define (sum-div n i)
  (cond [(= i 1) 1]
        [(= (remainder n i) 0)(+ i (sum-div n (- i 1)))]
        [else (sum-div n (- i 1))]))

(define (make-string-list N)
  (make-string-helper N '()))


(define (make-string-helper N L)
  (cond [(= N 0) (set! L (append L (list (~a"Finished")))) L]
        [(= N 1)(set! L (append L (list (~a "1 second"))))(make-string-helper (- N 1) L)]
        [else (set! L (append L (list (~a N " seconds"))))(make-string-helper (- N 1) L)]))







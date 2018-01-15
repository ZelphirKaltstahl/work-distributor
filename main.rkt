#lang racket

#|
This code is mostly adapted from code given as examples by the people from the Racket user group.
The group has been indispensable to help me understanding how places in Racket work and how to use them.
|#

;; outputs stuff on a channel rather than stdout stdin and stderr or other ports
;; This is used by child places to put things on the `out-ch` made by calling
;; (make-out-ch ...).
(define (place-output out-ch . args)
  (place-channel-put out-ch args))

;; Gets the pair of channels for the main place.
;; Then starts a new thread to `printf` whatever is sent by child places on the out-ch and
;; arrives on the in-ch for the main place.
;; Then returns the counterpart of the in-ch, the out-ch to give it to child places,
;; so that they can `printf` command line output by putting messages on the `out-ch`.
;; The messages the child places put on the `out-ch` will become available on the `in-ch`.
;; When that happens the (sync in-ch) running in the thread will notice,
;; that the event represented by the channel `in-ch` is ready and
;; will use that as something to `printf`.
(define (make-out-ch)
  (define-values (in-ch out-ch)
    (place-channel))
  (thread (λ ()
            (let loop ()
              (apply printf (sync in-ch))
              (newline)
              (loop))))
  out-ch)
;; Intersting to know: This way one can control what happend with the stuff that is put on `out-ch`.
;; We could define arbitrary code to run here, when we receive a message on the `in-ch`.
;; In this case we only want to print messages from the child places.


;; The message handling protocol defined how the child places will react on messages.
(define (message-handling-protocol place-id get-work-channel terminate-channel out-channel)
  ;; Immediately enter the loop.
  (let message-handling-loop ()
    ;; as soon as one of the handled events is ready it is the result of sync

    ;; https://docs.racket-lang.org/reference/sync.html
    ;; "Certain kinds of objects double as events, including ports and threads."
    ;; So ports are OK here!

    ;; However, we are not waiting for a result here,
    ;; but only use this as a synchronization method,
    ;; so that we can run the loop again and
    ;; wait for the next message on one of the channels.
    (sync (handle-evt terminate-channel
                      ;; handle.
                      ;; This λ is called with the synchronization result of
                      ;; `terminate-pch`. The synchronization result of a channel
                      ;; is the next message that is ready on the channel. The λ
                      ;; is called with the next message.
                      ;; However, we do not use the message. If we receive any
                      ;; message on `terminate-pch` we terminate by simply not
                      ;; calling the `message-handling-loop` any longer.
                      (λ (_) (place-output out-channel
                                           "Place ~s is going to finish now."
                                           place-id)))
          ;; We have access to `get-work-pch` here, because we are using
          ;; `place/context` to create the place which allows us to use
          ;; "free lexical variables".
          ;; On the `get-work-pch` we will receive additional work to be done.
          (handle-evt get-work-channel
                      (λ (data)
                        ;; In here one could do more to distinguish different types of messages.
                        ;; For example using pattern matching against `data`.
                        ;; Each message could be tagged, so that we know what to do with it.
                        #;(place-output out-channel
                                      "Place ~s got the following work: ~a."
                                      place-id
                                      data)
                        (place-channel-put get-work-channel (do-work place-id data))
                        (message-handling-loop))))))

(define (do-work place-id data)
  ;; some busy work to be able to see how many cores are used
  (for ([i (range 5000)])
    (expt i (add1 i)))
  (list 'result place-id data))

(define (start-n-places n-places get-work-pch out-ch)
  ;; Start as many places, as we have processing units.
  (for/list ([place-id n-places])
    (define a-place
      ;; `terminate-pch` is a place channel, on which we can send a message to the place.
      ;; Inside this place all top-level bindings of the enclosing module
      ;; and the `terminate-pch` are visible.
      (place/context terminate-pch
                     (place-output out-ch "place starting")
                     ;; protocol for message processing
                     ;; internally runs the message-processing-loop
                     (message-handling-protocol place-id
                                                get-work-pch
                                                terminate-pch
                                                out-ch)))
    ;; When the place is created let us know about it by putting a message on the channel.
    (place-output out-ch "created place ~s" place-id)
    ;; Return the place as a result, to be added to the list created by `for/list`.
    a-place))


;; (place-channel-put a-place 'terminate) will put 'terminate on the place channel
;; which was specified in the call to (place/context terminate-pch ...).
;; Since we do not care about what is put on that channel and
;; always react with termination of the place, this leads to termination of the places.
(define (stop-places places)
  ;; simply terminate all places
  (for ([a-place (in-list places)])
    (place-channel-put a-place 'terminate))
  ;; wait for termination
  (for ([a-place (in-list places)])
    (place-wait a-place)))


(define (retrieve-n-results-from-channel send-work-pch num-messages)
  (let loop ([done 0]
             [results '()])
    #;(place-output out-ch "current results: ~a" results)
    ;; The whole computation is only finished,
    ;; if all messages resulted a message from the places.
    (cond [(< done num-messages)
           #;(place-output out-ch "message-counter: ~s" done)
           (loop (add1 done)
                 ;; collect work results
                 (let ([single-result (sync send-work-pch)])
                   (cons single-result results)))]
          [else #;(place-output out-ch "results: ~s" results)
                results])))

(define (put-work-on-channel work send-work-pch)
  (for/fold ([n 0])  ; accumulate in n
            ([message work])  ; iterate over messages
    (place-channel-put send-work-pch message)
    (add1 n)))

(define (main n-places [messages '(the quick brown fox jumped over the lazy dogs)])
  ;; This must not be at the module level, or each place will get its own version.
  ;; (Why would that happen? – Because the places would see the top-level module bindings,
  ;; because they are lifted to top-level and they do not share any data.
  ;; This means they would each get these values in their own Racket instance.)
  ;; These channels are the channels through which we will put work to the child places and
  ;; receive the results from the places.
  (define-values (send-work-pch get-work-pch)
    (place-channel))

  ;; Make an out channel which has a counterpart which is printed in another thread.
  ;; Messages going in this out channel will result in messages coming out the counterpart channel.
  (define out-ch (make-out-ch))

  ;; Create the places and let them know the `get-work-pch`,
  ;; so that they know where to get more work form.
  ;; Also let them output anything to this place's outbount channel. (Why?)
  (define places (start-n-places n-places get-work-pch out-ch))

  ;; Give the places some time to start up.
  (sleep 1)

  ;; Count sent messages which are put on the `send-work-pch`.
  (define num-messages
    (put-work-on-channel messages send-work-pch))

  (place-output out-ch "main has no more work data")

  ;; We listen on the `send-work-pch` because it is the counterpart of the `get-work-pch`,
  ;; on which the child places will put results.
  (define results
    (retrieve-n-results-from-channel send-work-pch num-messages))

  (place-output out-ch "all work reports finished")
  (place-output out-ch "The results are ~s." results)

  (stop-places places))

;; Is this module* thing required?
(module* main #f
  (main (processor-count)))

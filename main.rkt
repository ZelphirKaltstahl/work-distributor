#lang racket

#|
About this code:
  This code is mostly adapted from code given as examples by the people from the Racket user group.
  The group has been indispensable to help me understanding how places in Racket work and how to use them.

Issues of this implementation:
  (1)
  This implementation assumes that a channel will finish without failure.
  A failure might lead to hanging of the implementation.

  (2)
  Unlikely case, but we are talking about concurrency here ...:
  What if the places grab work from the channel faster than the main place
  can put work on the channel. Wont they think they are already done?

  Solution idea:
  Let the main place put a message on the receive-work/send-results-channel
  to signal that there is no further work, for each place one such message,
  so that they stop looking for more work.

Improvement ideas:
  The terminate channel might be redundant, if we define a message protocol,
  which interprets messages, that signal that there is no more work to distribute to any place.
|#

;; outputs stuff on a channel rather than stdout stdin and stderr or other ports
;; This is used by child places to put things on the `out-ch` made by calling
;; (make-out-channel ...).
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
(define (make-out-channel)
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
(define (message-handling-protocol place-id
                                   receive-work/send-results-channel
                                   terminate-channel
                                   out-channel)
  ;; Procedure which does work and returns the result of the work.
  ;; It is called from within the message-handling-loop.
  (define (do-work data)
    ;; TODO: Do real work here.
    ;; some busy work to be able to see how many cores are used
    (for ([i (range 5000)])
      (expt i (add1 i)))
    (list 'result place-id data))

  ;; This loop is run when the place receives a termination message on the terminate-channel.
  (define (post-terminate-loop)

    ;; If there is more work remaining do it, otherwise really terminate by exiting the loop,
    ;; by not calling it again.
    (let ([remaining-work-result
           ;; This checks whether there is any work remaining by reacting on messages
           ;; on the `receive-work/send-results-channel`.
           ;; However, doing so requires waiting for a message.
           ;; There is no `peek` operation for place-channels like the peek for a stack.
           ;; Once it receives a message it needs to call `do-work` so that
           ;; the message is processed.

           ;; It could be that there is no remaining message.
           ;; If the waiting for messages were blocking,
           ;; this could cause the place to hang in an endless waiting state.
           ;; This is why we `sync/timeout`.
           ;; If there is a message remaining, do the work and return the result.

           ;; Do another job if any are remaining, but don't block.
           ;; sync/timeout with 0 as timeout is equal to timeout being (λ () #f).
           ;; If no event is ready timeout is called in tail position.
           ;; Synchronization result is determined by calling do-work with the work item.

           ;; Issue(?):
           ;; What if the places grab work from the channel faster than the main place
           ;; can put work on the channel. Wont they think they are already done?

           ;; Solution idea:
           ;; Let the main place put a message on the receive-work/send-results-channel
           ;; to signal that there is no further work, for each place one such message,
           ;; so that they stop looking for more work.
           (sync/timeout 0 (wrap-evt receive-work/send-results-channel
                                     do-work))])
      (cond [remaining-work-result (place-channel-put receive-work/send-results-channel
                                                      remaining-work-result)
                                   (post-terminate-loop)]
            [else (place-channel-put receive-work/send-results-channel
                                     'finished)
                  (place-output out-channel
                                "Place ~s is going to finish now."
                                place-id)])))

  ;; Immediately enter the message-handling-loop when message-handling-protocol is called.
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
                      (λ (_)
                        (post-terminate-loop)
                        (place-output out-channel
                                      "Place ~s is going to finish now."
                                      place-id)))
          ;; We have access to `get-work-pch` here, because we are using
          ;; `place/context` to create the place which allows us to use
          ;; "free lexical variables".
          ;; On the `get-work-pch` we will receive additional work to be done.
          (handle-evt receive-work/send-results-channel
                      (λ (data)
                        ;; In here one could do more to distinguish different types of messages.
                        ;; For example using pattern matching against `data`.
                        ;; Each message could be tagged, so that we know what to do with it.
                        #;(place-output out-channel
                                      "Place ~s got the following work: ~a."
                                      place-id
                                      data)
                        (place-channel-put receive-work/send-results-channel
                                           (do-work data))
                        (message-handling-loop))))))

(define (start-n-places n-places receive-work/send-results-channel out-ch)
  ;; Start as many places, as we have processing units.
  (for/list ([place-id n-places])
    (define a-place
      ;; `terminate-channel` is a place channel, on which we can send a message to the place.
      ;; Inside this place all top-level bindings of the enclosing module
      ;; and the `terminate-channel` are visible.
      (place/context terminate-channel
                     (place-output out-ch "place starting")
                     ;; protocol for message processing
                     ;; internally runs the message-processing-loop
                     (message-handling-protocol place-id
                                                receive-work/send-results-channel
                                                terminate-channel
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

;; Note: This could be seen as the main place's message protocol.
;; Maybe the naming should reflect that.
(define (retrieve-results places# receive-results-channel out-channel)
  (let loop ([done 0]
             [results '()])
    (cond [(< done places#)
           (let ([single-result (sync receive-results-channel)])
             (match single-result
               ;; Places must send a 'finished message when they are done with all work!
               ;; Warning: Here lies an issue: What if the place crashes,
               ;;          or otherwise fails to put such message on the channel?
               ['finished (loop (add1 done) results)]
               ;; Anything else will be considered a result.
               [(list 'result place-id single-result) (loop done (cons single-result results))]
               [anything-else (place-output out-channel
                                            "message from child place not understood: ~a"
                                            anything-else)
                              (loop done results)]))]
          [else results])))

(define (put-work-on-channel work send-work-pch)
  (for/fold ([n 0])  ; accumulate in n
            ([message work])  ; iterate over messages
    (place-channel-put send-work-pch message)
    (add1 n)))

(define (main places# [messages (range 50)#;'(the quick brown fox jumped over the lazy dogs)])
  ;; This must not be at the module level, or each place will get its own version.
  ;; (Why would that happen? – Because the places would see the top-level module bindings,
  ;; because they are lifted to top-level and they do not share any data.
  ;; This means they would each get values in their respective Racket instance.)
  ;; These channels are the channels through which we will put work to the child places and
  ;; receive the results from the places.
  (define-values (send-work/receive-results-channel receive-work/send-results-channel)
    (place-channel))

  ;; Make an out channel which has a counterpart which is printed in another thread.
  ;; Messages going in this out channel will result in messages coming out the counterpart channel.
  (define out-channel (make-out-channel))

  ;; Create the places and let them know the `get-work-pch`,
  ;; so that they know where to get more work form.
  ;; Also let them output anything to this place's outbount channel. (Why?)
  (define places (start-n-places places#
                                 receive-work/send-results-channel
                                 out-channel))

  ;; Sleep for a short time to let places initialize.
  ;; (TODO: not sure this is required)
  (sleep 1)

  ;; Put all messages on the channel for the places to grab them and do their work.
  (for ([message messages])
    (place-channel-put send-work/receive-results-channel message))

  (place-output out-channel "main has no more work data")

  ;; We listen on the `send-work-pch` because it is the counterpart of the `get-work-pch`,
  ;; on which the child places will put results.
  ;; TODO: IDEA: Last message from place is a work done message and
  ;; we count work done = places as condition for all work to be finished.

  (place-output out-channel "letting places finish")
  (stop-places places)
  (place-output out-channel "places stopped")

  (place-output out-channel "collecting results ...")
  (define results
    (retrieve-results places#
                      send-work/receive-results-channel
                      out-channel))
  (place-output out-channel "all work results collected")

  (place-output out-channel "The results are ~s." results)
  results)

;; Is this module* thing required? –
;; Yes, because places are lifted.
;; This means each place would in its own Racket instance see a plain (main ...) call and run that,
;; if the call was not inside a submodule.
;; This would lead to an endless loop with each new place calling (main ...) again,
;; creating more places.
(module* main #f
  (main (processor-count)))

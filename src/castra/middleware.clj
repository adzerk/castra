(ns castra.middleware
  (:require [cognitect.transit :as t]
            [ring.util.request :as q :refer [body-string]]
            [clojure.set       :as s :refer [intersection difference]]
            [castra.core       :as r :refer [ex? dfl-ex *pre* *request*
                                             *session* *validate-only*]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(defn- ex->clj [e]
  (let [e (if (ex? e) e (dfl-ex e))]
    {:message (.getMessage e)
     :data    (ex-data e)}))

(defn- safe-symbol
  "Like `clojure.core/symbol`, but if the provided value cannot be coerced into
   a symbol, returns nil instead of throwing an exception."
  [value]
  (try
    (symbol value)
    (catch Throwable _
      nil)))

(defn- do-rpc [vars [f & args]]
  (let [bad!  #(throw (ex-info
                        "RPC endpoint not found"
                        {:error-code :castra/rpc-endpoint-not-found
                         :endpoint   (or (safe-symbol f) f)}))
        fun   (let [sym (safe-symbol f)]
                (or (and sym (resolve sym))
                    (bad!)))]
    (when-not (contains? vars fun) (bad!))
    (apply fun args)))

(defn- select-vars [nsname & {:keys [only exclude]}]
  (let [to-var    #(resolve (symbol (str nsname) (str %)))
        to-vars   #(->> % (map to-var) (keep identity) set)
        var-pubs  #(do (require %) (vals (ns-publics %)))
        vars      (->> nsname var-pubs set)
        only      (if (seq only) (to-vars only) vars)
        exclude   (if (seq exclude) (to-vars exclude) #{})]
    (-> vars (intersection only) (difference exclude))))

(def clj->json
  (atom #(let [out (ByteArrayOutputStream. 4096)]
           (t/write (t/writer out :json) %2)
           (.toString out))))

(def json->clj
  (atom #(-> (ByteArrayInputStream. (.getBytes %2)) (t/reader :json) t/read)))

(defn expression
  [body-keys req]
  (or (some req body-keys) (@json->clj req (body-string req))))

(defn response
  [body-keys req data]
  (if (some req body-keys) data (@clj->json req data)))

(defn headers
  [{:keys [body] :as _req} head extra]
  (merge head (when-not (coll? body) extra)))

(defn- castra-req?
  [req]
  (and (get-in req [:headers "x-castra-csrf"])
       (= :post (:request-method req))))

(defn wrap-castra [handler & [opts & more :as namespaces]]
  (let [{:keys [body-keys state-fn exception-handler
                parsed-request-body-handler]
         :as opts}
        (when (map? opts) opts)

        nses
        (if opts more namespaces)

        head
        {"X-Castra-Tunnel" "transit"}

        seq*
        #(or (try (seq %) (catch Throwable _)) [%])

        vars
        (fn [] (->> nses (map seq*) (mapcat #(apply select-vars %)) set))]
    (fn [req]
      (if-not (castra-req? req)
        (handler req)
        (binding [*print-meta*    true
                  *pre*           true
                  *request*       req
                  *session*       (atom (:session req))
                  *validate-only* (= "true" (get-in req [:headers "x-castra-validate-only"]))]
          (let [h
                (headers req head {"Content-Type" "application/json;charset=utf-8"})

                f
                #(let [parsed-req-body
                       (try
                         (expression body-keys req)
                         (catch Throwable e
                           (throw (ex-info
                                    "Error reading request body"
                                    {:error-code :castra/read-error
                                     :error      (ex->clj e)}))))]
                   (when parsed-request-body-handler
                     (parsed-request-body-handler parsed-req-body))
                   (do-rpc (vars) parsed-req-body))

                d
                (try (response
                       body-keys
                       req
                       {:result (f) :state (when state-fn (state-fn))})
                     (catch Throwable e
                       (when exception-handler
                         (exception-handler e))
                       (response body-keys req {:error (ex->clj e)})))]
            {:status 200, :headers h, :body d, :session @*session*}))))))

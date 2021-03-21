# vellum-postmodern

This system adds a basic support for constructing vellum tables from the queries to the postgres database. S-SQL syntax is supported.

Example
```
(vellum:copy-from :postmodern
`(:select 'primary_dgns_cd 'anchip_icd10d_codes
          'pac1 'readm_90_days 'death_in_postdisch
          'history_d_10
      :from 'lds2019
      :inner-join 'first_pacs
      :on (:= (:type 'lds2019.episode_id varchar)
               'first_pacs.episode_id)
      :where (:and (:= 'mdc ,mdc)
                   (:not 'death_in_anchor)))
 :columns '(primary-dgns-cd anchip-icd10d-codes pac1 readm-90-days death-in-postdisch history-d-10))
```

Inserting data into postgres data base is currently unsupported.

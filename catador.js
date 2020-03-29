const { of, from } = require("rxjs");
const fetch = require("node-fetch");
const {
  switchMap,
  map,
  toArray,
  groupBy,
  mergeMap,
  startWith,
  scan,
  pairwise,
  flatMap,
  expand,
  tap,
  delay
} = require("rxjs/operators");

const fs = require("fs");

let _ultimaAtualizacaoDados = null;

const fromFetch = url =>
  from(fetch(url)).pipe(flatMap(response => response.json()));

function pegar(url) {
  const getData = url => fromFetch(url);
  const obs$ = getData(url).pipe(
    map(response => response.tables),
    tap(tables => {
      _ultimaAtualizacaoDados = new Date(
        tables.find(table => table.name == "caso").import_date
      );
    }),
    switchMap(tables =>
      getData(
        tables.find(table => table.name == "caso").data_url + "?page_size=10000"
      )
    ),
    expand(response =>
      of(null).pipe(
        delay(500),
        switchMap(() => {
          return response.next ? getData(response.next) : of();
        })
      )
    ),
    flatMap(o => o.results),
    toArray(),
    switchMap(array => {
      return from(
        array.sort((a, b) => {
          a = Date.parse(a.date);
          b = Date.parse(b.date);
          if (!a) return -1;
          if (!b) return 1;
          return a - b;
        })
      );
    }),
    groupBy(citycase => citycase.city_ibge_code),
    mergeMap(city =>
      city.pipe(
        startWith({ deaths: 0, confirmed: 0 }),
        scan(
          (acc, value) => {
            const newValue = { ...value };
            if (value.deaths < acc.deaths)
              console.log(
                `Mortes diminuiram do dia ${acc.date}: ${acc.deaths} para ${value.date}: ${value.deaths}. (IBGE: ${value.city_ibge_code})`
              );
            newValue.deaths = Math.max(acc.deaths, value.deaths);
            newValue.confirmed = Math.max(acc.confirmed, value.confirmed);
            newValue.date = value.date;
            return newValue;
          },
          { deaths: 0, confirmed: 0, date: null, city_ibge_code: null }
        ),
        pairwise(),
        map(pair => {
          return {
            ...pair[1],
            confirmeddiff: pair[1].confirmed - pair[0].confirmed,
            deathsdiff: pair[1].deaths - pair[0].deaths
          };
        })
      )
    ),
    toArray()
  );
  return obs$;
}

pegar("https://brasil.io/api/dataset/covid19").pipe(
).subscribe(
  (dados) => {
    dados.ultimaAtualizacaoDados = _ultimaAtualizacaoDados;
    fs.writeFileSync("dados/dados.json", JSON.stringify(dados));
  },
  console.error
);

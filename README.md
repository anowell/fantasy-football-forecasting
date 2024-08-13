# Fantasy Football Foresights (FFF)


[Pola.rs](https://pola.rs) exploration of NFL [play-by-play data](https://github.com/nflverse/nflverse-data)

This is mostly an attempt at having an unfair advantage in a FF league with my bro.

It's very exploratory/experimental currently. Some ideas that might get implemented:

- [x] Reliably calculate fantasy points using most widely-supported scoring systems
- [x] Explore fantasy scores for players over custom windows (e.g. last 5 weeks)
- [ ] Explore fantasy score trends for players against certain opponents
- [ ] Explore the impact certain defenses have on fantasy scoring
- [ ] Explore value-over-replacement [VOR](https://github.com/jjti/ff) calculation
- [ ] Explore heuristics that could be used to pick drafts, lineups, and waiver-wire additions.


## Getting started

- [Rust](rustup.rs)
- [Polars CLI](https://github.com/pola-rs/polars-cli) (also recommend [jq](https://jqlang.github.io/jq/))
- [Just](https://github.com/casey/just) (runner)

Download the play-by-play dataset: `just download-pbp 2023`

Running `fff`
```
  just run --help

  # or
  cargo build --release
  target/release/fff --help
```


```
Usage: fff [OPTIONS]

Options:
  -v, --verbose...           
  -y, --year <YEAR>          Loads data for a given year [default: 2023]
  -t, --team <TEAM>          Filter by team
  -p, --pos <POSITION>       Filter by position
  -w, --week <WEEKS>         Filtering week number or range (e.g. 3 or 3-5)
  -x, --exclude <EXCLUDE>    
      --score <SCORE>        Calculate fantasy score
      --score-by <SCORE_BY>  Choose how to aggregate scores [default: player] [possible values: player-game, player, game]
  -h, --help                 Print help
  -V, --version              Print version
````


You can also just use polars to explore the data:

```
$ just query "select qtr, time, desc from plays where week = 2 AND posteam = 'SEA' AND touchdown = 1.0" -o json
{"qtr":1.0,"time":"07:07","desc":"(7:07) (Shotgun) 9-K.Walker up the middle for 1 yard, TOUCHDOWN."}
{"qtr":3.0,"time":"14:22","desc":"(14:22) (No Huddle, Shotgun) 9-K.Walker left tackle for 3 yards, TOUCHDOWN."}
{"qtr":4.0,"time":"10:45","desc":"(10:45) 7-G.Smith pass short left to 16-T.Lockett for 3 yards, TOUCHDOWN."}
{"qtr":5.0,"time":"05:48","desc":"(5:48) (Shotgun) 7-G.Smith pass short right to 16-T.Lockett for 6 yards, TOUCHDOWN."}
````

## Why Rust?

~It's better than Python.~

I know Rust best. I like it. That's reason enough.

Or: My Python is pretty lousy. My Ruby has been getting Rusty.

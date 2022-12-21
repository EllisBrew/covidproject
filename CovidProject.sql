--COLLECT THE DATA WE WANT
SELECT *
from master..coviddeaths
ORDER BY 1,2

--TOTAL CASES vs TOTAL DEATHS
--shows the likelihood of death if you contract covid in a given country
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_percentage
from master..coviddeaths
WHERE [location] LIKE 'united kingdom'
ORDER BY 1,2


--Total cases vs population
--shows percentage of population that have been infected
SELECT location, date, population,total_cases, (total_cases/population)*100 AS infected_percentage
from master..coviddeaths
WHERE [location] LIKE 'united kingdom'
ORDER BY 1,2

--looking at countries with highest infection rate compared to population
SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population))*100 AS highest_infected_percentage
from master..coviddeaths
WHERE continent IS NOT NULL
GROUP BY population, [location]
ORDER BY highest_infected_percentage DESC

--showing continents with highest death count
SELECT continent, MAX(total_deaths) AS total_death_count
from master..coviddeaths
WHERE continent IS NOT NULL
GROUP BY [continent]
ORDER BY total_death_count DESC

--showing the countries with highest death count per population
SELECT location, MAX(total_deaths) AS total_death_count
from master..coviddeaths
WHERE continent IS NOT NULL
GROUP BY [location]
ORDER BY total_death_count DESC


--GLOBAL NUMBERS
SELECT date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 as death_percentage
from master..coviddeaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1,2

--looking at total population vs vaccination

--SELECT dea.continent, dea.location, dea.date, dea.population, vax.new_vaccinations, SUM(vax.new_vaccinations) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS rolling_vaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, SUM(vax.new_people_vaccinated_smoothed) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS rolling_people_vaxed
from master..coviddeaths dea
JOIN master..covidvax vax
    ON dea.location = vax.location AND dea.date = vax.date
WHERE dea.continent IS NOT NULL AND dea.location LIKE 'united kingdom'
ORDER BY 2,3


-- USE CTE

WITH PopvsVac (continent, location, date, population, rolling_people_vaxed)
AS (
SELECT dea.continent, dea.location, dea.date, dea.population, SUM(vax.new_people_vaccinated_smoothed) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS rolling_people_vaxed
from master..coviddeaths dea
JOIN master..covidvax vax
    ON dea.location = vax.location AND dea.date = vax.date
WHERE dea.continent IS NOT NULL

)

SELECT *, (rolling_people_vaxed/population)*100 AS vax_percentage
FROM PopvsVac
WHERE location LIKE 'united states'


--TEMP TABLES

DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated
(
    continent nvarchar(50),
    location nvarchar(50), 
    date date, 
    population float, 
    new_people_vaccinated_smoothed float, 
    rolling_people_vaxed float,
)
INSERT INTO #PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vax.new_people_vaccinated_smoothed, SUM(vax.new_people_vaccinated_smoothed) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS rolling_people_vaxed
from master..coviddeaths dea
JOIN master..covidvax vax
    ON dea.location = vax.location AND dea.date = vax.date
WHERE dea.continent IS NOT NULL
ORDER BY 2,3

DROP TABLE IF EXISTS #PercentPopulationInfected
CREATE TABLE #PercentPopulationInfected
(
    continent nvarchar(50),
    location nvarchar(50), 
    date date, 
    population float,
    total_cases float, 
    infected_percentage float,
)
INSERT INTO #PercentPopulationInfected
SELECT dea.continent, dea.location, dea.date, dea.population, dea.total_cases, (dea.total_cases/dea.population)*100 AS infected_percentage
from master..coviddeaths dea
WHERE dea.continent IS NOT NULL
ORDER BY 2,3

--CREATING VIEWS TO STORE DATA FOR VISUALISATION

CREATE VIEW PercentPopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vax.new_people_vaccinated_smoothed, SUM(vax.new_people_vaccinated_smoothed) OVER (Partition by dea.location ORDER BY dea.location, dea.date) AS rolling_people_vaxed
from master..coviddeaths dea
JOIN master..covidvax vax
    ON dea.location = vax.location AND dea.date = vax.date
WHERE dea.continent IS NOT NULL

CREATE VIEW PercentPopulationInfected AS
SELECT dea.continent, dea.location, dea.date, dea.population, dea.total_cases, (dea.total_cases/dea.population)*100 AS infected_percentage
from master..coviddeaths dea
WHERE dea.continent IS NOT NULL


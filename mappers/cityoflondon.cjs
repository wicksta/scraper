module.exports = {
  scraperName: "idox_cityoflondon",

  /**
   * @param {object} unified - UNIFIED scrape JSON
   * @param {object} ctx - helpers + optional fixed context
   * @returns {{ planit: object, warnings: string[], unmapped: object }}
   */
  async mapToPlanit(unified, ctx) {
    const warnings = [];
    const unmapped = {};
    const { extractUkPostcode, resolvePostcodeViaOnspd } = require("../library.cjs");

    const summary = unified.tabs?.summary?.extracted || {};
    const details = unified.tabs?.further_information?.extracted || {};
    const dates = unified.tabs?.important_dates?.extracted || {};

    const sTable = summary.tables?.simpleDetailsTable || {};
    const dTable = details.tables?.applicationDetails || {};
    const dateTable = dates.tables?.simpleDetailsTable || {};

    const ref = ctx.pickFirst(summary.headline?.reference, sTable.reference, unified.query);
    const ppId = ctx.pickFirst(sTable.alternative_reference, sTable.planning_portal_reference);

    const address = ctx.pickFirst(summary.headline?.address, sTable.address);

    const dateReceived = ctx.normaliseIdoxDateToISO(
      ctx.pickFirst(sTable.application_received, dateTable.application_received_date),
    );

    const dateValidated = ctx.normaliseIdoxDateToISO(
      ctx.pickFirst(sTable.application_validated, dateTable.application_validated_date),
    );

    const decidedDate = ctx.normaliseIdoxDateToISO(
      ctx.pickFirst(sTable.decision_issued_date, dateTable.decision_made_date, dateTable.decision_issued_date),
    );

    // City of London commonly exposes these labels instead of target_date.
    const targetDate = ctx.normaliseIdoxDateToISO(
      ctx.pickFirst(dateTable.determination_deadline, dateTable.internal_target_date, dateTable.target_date),
    );

    const description = ctx.compactWhitespace(ctx.pickFirst(summary.headline?.description, sTable.proposal));

    const status = ctx.pickFirst(sTable.status, summary.headline?.decision_badge);
    const appState = ctx.pickFirst(sTable.status, sTable.decision);
    const appType = ctx.pickFirst(dTable.application_type, null);

    const agentName = ctx.pickFirst(dTable.agent_name, null);
    const agentCompany = ctx.pickFirst(dTable.agent_company_name, dTable.agent_company, null);
    const agentAddress = ctx.pickFirst(dTable.agent_address, null);
    const applicantName = ctx.pickFirst(dTable.applicant_name, null);
    const caseOfficer = ctx.pickFirst(dTable.case_officer, null);

    const planit = {
      uid: ref || null,
      name: ref || null,

      address: address || null,
      postcode: extractUkPostcode(address) || null,
      ward_name: ctx.pickFirst(dTable.ward, null),

      area_id: null,
      area_name: ctx.area_name || null,
      ons_code: ctx.ons_code || null,

      app_type: appType,
      app_size: null,

      app_state: appState || null,
      status: status || null,

      associated_id: null,
      description: description || null,

      link: unified.tabs?.summary?.url || null,
      source_url: unified.tabs?.summary?.url || null,
      docs_url: null,
      comment_url: null,
      map_url: null,

      planning_portal_id: ppId || null,

      agent_name: agentName || null,
      agent_company: agentCompany || null,
      agent_address: agentAddress || null,

      applicant_name: applicantName || null,
      case_officer: caseOfficer || null,

      location_x: null,
      location_y: null,
      easting: null,
      northing: null,
      lat: null,
      lng: null,

      n_documents: null,
      n_comments: null,
      n_statutory_days: null,

      date_received: dateReceived,
      date_validated: dateValidated,
      start_date: dateValidated || dateReceived || null,
      target_decision_date: targetDate,
      decided_date: decidedDate,
      consulted_date: null,

      last_scraped: new Date().toISOString(),
      scraper_name: ctx.scraper_name,
      url: unified.tabs?.summary?.url || null,

      cannot_find: 0,
    };

    // Only if we don't already have coords: fill from postcode via ONSPD.
    if (planit.lat == null && planit.lng == null && planit.postcode) {
      const geo = await resolvePostcodeViaOnspd(planit.postcode);
      if (geo.success) {
        planit.lat = geo.lat;
        planit.lng = geo.long;
      }
    }

    // Populate location_x/location_y from lat/lng (preferred for PlanIt consumers).
    // Assumption: location_x = lat, location_y = lng.
    if (planit.location_x == null && planit.location_y == null) {
      if (planit.lat != null && planit.lng != null) {
        planit.location_x = planit.lat;
        planit.location_y = planit.lng;
      }
    }

    if (planit.n_statutory_days == null && planit.date_validated && planit.target_decision_date) {
      const start = new Date(`${planit.date_validated}T00:00:00Z`);
      const end = new Date(`${planit.target_decision_date}T00:00:00Z`);
      if (!Number.isNaN(start.getTime()) && !Number.isNaN(end.getTime())) {
        planit.n_statutory_days = Math.round((end - start) / 86400000);
      }
    }

    if (!planit.uid) warnings.push("Missing uid/reference (could not map ref).");
    if (!planit.start_date) warnings.push("Missing start_date; DB requires NOT NULL. Mapping set it null.");
    if (!planit.description) warnings.push("Missing description/proposal.");
    if (!planit.address) warnings.push("Missing address.");

    unmapped.source = {
      headline: summary.headline,
      simpleDetailsTable: sTable,
      applicationDetails: dTable,
      datesTable: dateTable,
    };

    return { planit, warnings, unmapped };
  },
};

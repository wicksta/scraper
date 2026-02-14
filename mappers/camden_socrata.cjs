module.exports = {
  scraperName: "camden_socrata",

  /**
   * Mapper for UNIFIED JSON produced by scraper_camden_socrata.cjs.
   *
   * @param {object} unified
   * @param {object} ctx
   * @returns {{ planit: object, warnings: string[], unmapped: object }}
   */
  async mapToPlanit(unified, ctx) {
    const warnings = [];
    const unmapped = {};

    const summary = unified.tabs?.summary?.extracted || {};
    const details = unified.tabs?.further_information?.extracted || {};
    const dates = unified.tabs?.important_dates?.extracted || {};

    const sTable = summary.tables?.simpleDetailsTable || {};
    const dTable = details.tables?.applicationDetails || {};
    const dateTable = dates.tables?.simpleDetailsTable || {};

    const ref = ctx.pickFirst(summary.headline?.reference, sTable.reference, unified.query);
    const address = ctx.pickFirst(summary.headline?.address, sTable.address);
    const description = ctx.compactWhitespace(ctx.pickFirst(summary.headline?.description, sTable.proposal));

    // Socrata scraper emits ISO date strings (YYYY-MM-DD) already, but keep normalisation.
    const dateReceived = ctx.normaliseIdoxDateToISO(
      ctx.pickFirst(sTable.application_received, dateTable.application_received_date),
    );
    const dateValidated = ctx.normaliseIdoxDateToISO(
      ctx.pickFirst(sTable.application_validated, dateTable.application_validated_date),
    );
    const decidedDate = ctx.normaliseIdoxDateToISO(
      ctx.pickFirst(sTable.decision_issued_date, dateTable.decision_made_date),
    );
    const targetDate = ctx.normaliseIdoxDateToISO(ctx.pickFirst(dateTable.determination_deadline));

    // Status/stage vs decision outcome:
    // - system_status: stage (e.g. "Final Decision")
    // - decision_type: outcome (e.g. "Prior Approval Required - Approval Refused")
    const appState = ctx.pickFirst(sTable.status);
    const status = ctx.pickFirst(sTable.decision, sTable.status);

    const applicationType = ctx.pickFirst(dTable.application_type, sTable.application_type, null);
    const decisionLevel = ctx.pickFirst(dTable.expected_decision_level, null);

    const applicantName = ctx.pickFirst(dTable.applicant_name, null);
    const caseOfficer = ctx.pickFirst(dTable.case_officer, null);
    const wardName = ctx.pickFirst(dTable.ward, null);

    const spatial = details.raw?.spatial || {};
    const lat = spatial.latitude != null ? Number(spatial.latitude) : null;
    const lng = spatial.longitude != null ? Number(spatial.longitude) : null;
    const easting = spatial.easting != null ? Number(spatial.easting) : null;
    const northing = spatial.northing != null ? Number(spatial.northing) : null;

    const planit = {
      uid: ref || null,
      name: ref || null,

      address: address || null,
      postcode: null,
      ward_name: wardName || null,

      area_id: null,
      area_name: ctx.area_name || null,
      ons_code: ctx.ons_code || null,

      app_type: applicationType,
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

      planning_portal_id: null,

      agent_name: null,
      agent_company: null,
      agent_address: null,

      applicant_name: applicantName || null,
      case_officer: caseOfficer || null,

      location_x: null,
      location_y: null,
      easting: Number.isFinite(easting) ? easting : null,
      northing: Number.isFinite(northing) ? northing : null,
      lat: Number.isFinite(lat) ? lat : null,
      lng: Number.isFinite(lng) ? lng : null,

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

    // Non-PlanIt extras (kept for debugging/upstream enrichment)
    planit.decision_level = decisionLevel || null;

    if (!planit.uid) warnings.push("Missing uid/reference (could not map ref).");
    if (!planit.start_date) warnings.push("Missing start_date; mapping set it null.");
    if (!planit.description) warnings.push("Missing description/proposal.");
    if (!planit.address) warnings.push("Missing address.");

    unmapped.source = {
      headline: summary.headline,
      simpleDetailsTable: sTable,
      applicationDetails: dTable,
      datesTable: dateTable,
      spatial: spatial,
    };

    return { planit, warnings, unmapped };
  },
};

